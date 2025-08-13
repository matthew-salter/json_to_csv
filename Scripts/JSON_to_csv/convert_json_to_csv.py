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
    snippets = []
    buffer = []
    block = []
    brace_balance = 0
    current_block = ""
    lines = text.splitlines()

    logger.info("🧪 Parsing input file line-by-line...")

    for line in lines:
        stripped = line.strip()

        # JSON block detection
        if "{" in stripped and not current_block:
            current_block = stripped
            brace_balance = stripped.count("{") - stripped.count("}")
            continue
        elif current_block:
            current_block += "\n" + stripped
            brace_balance += stripped.count("{") - stripped.count("}")
            if brace_balance == 0:
                try:
                    parsed = json.loads(current_block)
                    snippets.append(parsed)
                    logger.info(f"✅ Parsed JSON block with {len(parsed)} keys.")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to parse block-style JSON: {e}")
                current_block = ""
            continue

        # Loose line-by-line block
        if re.match(r'^"\s*[^"]+\s*"\s*:\s*".*"\s*,?$', stripped):
            block.append(stripped)
        elif stripped == "" and block:
            try:
                joined = "{\n" + "\n".join(block) + "\n}"
                parsed = json.loads(joined)
                snippets.append(parsed)
                logger.info(f"✅ Parsed loose key:value block with {len(parsed)} keys.")
            except Exception as e:
                logger.warning(f"⚠️ Failed to parse flat block: {e}")
            block = []

    if block:
        try:
            joined = "{\n" + "\n".join(block) + "\n}"
            parsed = json.loads(joined)
            snippets.append(parsed)
            logger.info(f"✅ Parsed trailing block with {len(parsed)} keys.")
        except Exception as e:
            logger.warning(f"⚠️ Failed to parse final block: {e}")

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
    global_key_tracker = global_key_tracker or defaultdict(int)
    global_key_total = global_key_total or {}

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
        for obj in json_objects:
            flat = flatten_json(obj, key_tracker, key_total_count)
            merged_flat.update(flat)
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
