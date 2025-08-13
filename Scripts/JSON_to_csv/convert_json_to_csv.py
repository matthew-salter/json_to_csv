import os
import re
import json
from datetime import datetime
from io import BytesIO
from collections import defaultdict, Counter
import xlsxwriter
from supabase import create_client
from Engine.Files.read_supabase_file import read_supabase_file
from Engine.Files.write_supabase_file import write_supabase_file
from logger import logger

MERGE_JSON_SNIPPETS = True  # 🔧 Toggle this to merge all JSONs into one row

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_BUCKET = "panelitix"
SUPABASE_FOLDER = "JSON_to_csv/JSON_Input_File"
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_latest_input_file() -> str:
    logger.info("📂 Scanning Supabase input folder for .txt files...")
    try:
        response = supabase.storage.from_(SUPABASE_BUCKET).list(SUPABASE_FOLDER)
        txt_files = [f["name"] for f in response if f["name"].endswith(".txt")]
        if not txt_files:
            raise FileNotFoundError("No .txt files found in input folder.")
        txt_files.sort(reverse=True)
        latest = txt_files[0]
        logger.info(f"🕒 Latest input file detected: {latest}")
        return latest
    except Exception as e:
        logger.error(f"❌ Failed to list Supabase folder: {e}")
        raise

def split_multiple_jsons(text):
    """
    Robustly splits a plaintext file that may contain:
      - Multiple JSON objects one after another, separated by commas (trailing '},')
      - Proper block JSONs delimited by brace balance
      - Loose key:value flat blocks

    Key fixes:
      * Strips a trailing comma from each captured object before json.loads()
      * Recovers from partial parsing by attempting a per-block array fallback
      * Handles 'loose' key:value lines by rebuilding a valid JSON object
    """
    snippets = []
    lines = text.splitlines()

    logger.info("🧪 Parsing input file line-by-line...")

    current_block = ""
    brace_balance = 0

    flat_items = []  # for loose key:value lines

    # Regex to capture loose '"Key": <JSON_value>' lines with optional trailing comma
    loose_kv_re = re.compile(r'^\s*"([^"]+)"\s*:\s*(.+?)\s*,?\s*$')

    def finalize_json_block():
        """Finalize and parse the current brace-balanced JSON block."""
        nonlocal current_block
        if not current_block:
            return
        block = current_block.strip()

        # Remove a trailing comma if present (e.g., object followed by a comma in a top-level sequence)
        if block.endswith(','):
            block = block[:-1].rstrip()

        try:
            parsed = json.loads(block)
            if isinstance(parsed, dict):
                snippets.append(parsed)
                logger.info(f"✅ Parsed JSON block with {len(parsed)} keys.")
            else:
                # In rare cases a block might itself contain multiple objects separated by commas
                recovered = json.loads(f"[{block}]")
                recovered_count = 0
                for obj in recovered:
                    if isinstance(obj, dict):
                        snippets.append(obj)
                        recovered_count += 1
                logger.info(f"🛠️ Recovered {recovered_count} object(s) via array fallback (block).")
        except Exception as e:
            logger.warning(f"⚠️ Failed to parse block-style JSON: {e}. Trying array fallback for block...")
            try:
                recovered = json.loads(f"[{block}]")
                recovered_count = 0
                for obj in recovered:
                    if isinstance(obj, dict):
                        snippets.append(obj)
                        recovered_count += 1
                logger.info(f"🛠️ Recovered {recovered_count} object(s) via array fallback (block).")
            except Exception as e2:
                logger.error(f"❌ Could not parse block even with array fallback: {e2}")

        current_block = ""

    def finalize_loose_block():
        """Finalize and parse a loose key:value block rebuilt into a valid JSON object."""
        nonlocal flat_items
        if not flat_items:
            return
        try:
            obj_text = "{\n" + ",\n".join(flat_items) + "\n}"
            parsed = json.loads(obj_text)
            if isinstance(parsed, dict):
                snippets.append(parsed)
                logger.info(f"✅ Parsed loose key:value block with {len(parsed)} keys.")
        except Exception as e:
            logger.warning(f"⚠️ Failed to parse flat block: {e}")
        finally:
            flat_items = []

    for raw_line in lines:
        line = raw_line.strip()

        # If we are inside a brace-balanced JSON block, keep accumulating
        if current_block:
            current_block += "\n" + line
            brace_balance += line.count("{") - line.count("}")
            if brace_balance == 0:
                # End of this JSON block
                finalize_json_block()
            continue

        # If not in a block yet, check for the start of one
        if "{" in line:
            # If we were collecting loose lines, finalize them before starting a real block
            if flat_items:
                finalize_loose_block()

            current_block = line
            brace_balance = line.count("{") - line.count("}")
            if brace_balance == 0:
                # Single-line JSON object
                finalize_json_block()
            continue

        # Otherwise, try to collect loose key:value lines
        if not line:
            # blank line separates loose blocks
            finalize_loose_block()
            continue

        m = loose_kv_re.match(line)
        if m:
            key = m.group(1)
            raw_val = m.group(2).strip()
            # Ensure the value is valid JSON:
            if not (raw_val.startswith('"') or raw_val.startswith('{') or raw_val.startswith('[')
                    or raw_val in ('true', 'false', 'null') or re.match(r'^-?\d+(\.\d+)?$', raw_val)):
                raw_val = json.dumps(raw_val.strip('"'))
            flat_items.append(json.dumps(key) + ": " + raw_val)
        else:
            # Non-matching line inside a loose block: end the loose block
            finalize_loose_block()

    # Finalize any trailing constructs
    if current_block:
        finalize_json_block()
    if flat_items:
        finalize_loose_block()

    logger.info(f"🧩 Total JSON snippets parsed: {len(snippets)}")
    return snippets

def count_keys_across_all(json_objects):
    key_counts = Counter()

    def extract_keys(item):
        if isinstance(item, dict):
            for key, value in item.items():
                formatted = key.strip().lower().replace(" ", "_").replace("-", "_")
                key_counts[formatted] += 1
                extract_keys(value)
        elif isinstance(item, list):
            for sub in item:
                extract_keys(sub)

    for obj in json_objects:
        extract_keys(obj)

    return key_counts

def flatten_json(obj, global_key_tracker=None, global_key_total=None):
    flat_dict = {}

    # ✅ Use None checks so we don't reset trackers between calls
    if global_key_tracker is None:
        global_key_tracker = defaultdict(int)
    if global_key_total is None:
        global_key_total = {}

    current_section = 0
    sub_counter = 0
    current_sub_id = None

    def format_key(k):
        return k.strip().lower().replace(" ", "_").replace("-", "_")

    def clean_value(v):
        return str(v).replace("\n", "\\n") if isinstance(v, str) else str(v)

    def recurse(d, parent_key=None):
        nonlocal current_section, sub_counter, current_sub_id

        if isinstance(d, dict):
            for key, value in d.items():
                f_key = format_key(key)

                if f_key == "section_title":
                    current_section += 1
                    sub_counter = 0
                    current_sub_id = None
                    global_key_tracker[f_key] += 1
                    count = global_key_tracker[f_key]
                    total = global_key_total.get(f_key, 1)
                    col_name = f"{f_key}_{count}" if total > 1 else f_key
                    flat_dict[col_name] = clean_value(value)
                    continue

                if f_key == "sub_section_title":
                    sub_counter += 1
                    current_sub_id = f"{current_section}.{sub_counter}"
                    col_name = f"{f_key}_{current_sub_id}"
                    flat_dict[col_name] = clean_value(value)
                    continue

                if isinstance(value, dict):
                    recurse(value, parent_key=f_key)
                elif isinstance(value, list):
                    list_val = "\\n".join(
                        str(v).replace("\n", "\\n") if isinstance(v, str) else str(v)
                        for v in value
                    )
                    global_key_tracker[f_key] += 1
                    count = global_key_tracker[f_key]
                    total = global_key_total.get(f_key, 1)

                    if f_key.startswith("sub_") and current_sub_id:
                        col_name = f"{f_key}_{current_sub_id}"
                    else:
                        col_name = f"{f_key}_{count}" if total > 1 else f_key

                    flat_dict[col_name] = list_val
                else:
                    global_key_tracker[f_key] += 1
                    count = global_key_tracker[f_key]
                    total = global_key_total.get(f_key, 1)

                    if f_key.startswith("sub_") and current_sub_id:
                        col_name = f"{f_key}_{current_sub_id}"
                    else:
                        col_name = f"{f_key}_{count}" if total > 1 else f_key

                    flat_dict[col_name] = clean_value(value)

        elif isinstance(d, list):
            for item in d:
                recurse(item)

    recurse(obj)
    return flat_dict

def process_json_objects(json_objects, key_tracker, key_total_count):
    if MERGE_JSON_SNIPPETS:
        merged_flat = {}
        global_duplicate_counter = defaultdict(int)

        for obj in json_objects:
            flat = flatten_json(obj, key_tracker, key_total_count)
            for key, value in flat.items():
                global_duplicate_counter[key] += 1
                if global_duplicate_counter[key] == 1:
                    merged_flat[key] = value
                else:
                    merged_flat[f"{key}_{global_duplicate_counter[key]}"] = value

        return [merged_flat]
    else:
        rows = []
        for obj in json_objects:
            flat = flatten_json(obj, key_tracker, key_total_count)
            rows.append(flat)
        return rows
        
def convert_json_to_csv(_: dict) -> dict:
    logger.info("🚀 Starting JSON to XLSX conversion")

    try:
        input_filename = get_latest_input_file()
    except Exception as e:
        return {"error": str(e)}

    logger.info(f"📥 Input filename selected: {input_filename}")

    try:
        txt_content = read_supabase_file(f"JSON_Input_File/{input_filename}")
    except Exception as e:
        logger.error(f"❌ Failed to read input file: {e}")
        return {"error": str(e)}

    logger.info(f"📄 File content preview:\n{txt_content[:400]}")

    try:
        json_objects = split_multiple_jsons(txt_content)
        if not json_objects:
            raise ValueError("No valid JSON objects found in file.")
    except Exception as e:
        logger.error(f"❌ JSON parsing error: {e}")
        return {"error": f"Invalid JSON structure: {e}"}

    try:
        key_total_count = count_keys_across_all(json_objects)
        key_tracker = defaultdict(int)
        rows = process_json_objects(json_objects, key_tracker, key_total_count)

        seen_keys = set()
        ordered_keys = []
        for row in rows:
            for key in row.keys():
                if key not in seen_keys:
                    seen_keys.add(key)
                    ordered_keys.append(key)

        output_stream = BytesIO()
        workbook = xlsxwriter.Workbook(output_stream, {'in_memory': True})
        worksheet = workbook.add_worksheet()

        for col, key in enumerate(ordered_keys):
            worksheet.write(0, col, key)
        for row_idx, row in enumerate(rows, 1):
            for col_idx, key in enumerate(ordered_keys):
                worksheet.write(row_idx, col_idx, row.get(key, ""))

        workbook.close()
        output_stream.seek(0)
        full_xlsx = output_stream.read()
    except Exception as e:
        logger.error(f"❌ XLSX generation error: {e}")
        return {"error": str(e)}

    try:
        basename = input_filename.replace(".txt", "").replace("JSON_input_file_", "")
        timestamp_str = basename if basename else datetime.utcnow().strftime("%d-%m-%Y_%H-%M-%S")
    except Exception as e:
        timestamp_str = datetime.utcnow().strftime("%d-%m-%Y_%H-%M-%S")

    output_filename = f"xlsx_output_file_{timestamp_str}.xlsx"
    output_path = f"csv_Output_File/{output_filename}"

    try:
        write_supabase_file(output_path, full_xlsx)
    except Exception as e:
        logger.error(f"❌ Failed to write XLSX file: {e}")
        return {"error": str(e)}

    logger.info(f"✅ XLSX written to: {output_path}")
    return {"status": "success", "csv_path": output_path}

def run_prompt(payload: dict) -> dict:
    return convert_json_to_csv(payload)
