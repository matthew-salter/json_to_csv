import os
import json
from datetime import datetime
from io import BytesIO
from collections import defaultdict
import xlsxwriter
from supabase import create_client
from Engine.Files.read_supabase_file import read_supabase_file
from Engine.Files.write_supabase_file import write_supabase_file
from logger import logger

# 🔹 Setup Supabase client
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

def extract_json_blocks(text):
    blocks = []
    current_block = []
    inside_json = False
    brace_count = 0

    for line in text.splitlines():
        line = line.strip()
        if line.startswith("{"):
            inside_json = True
            brace_count = line.count("{") - line.count("}")
            current_block = [line]
        elif inside_json:
            brace_count += line.count("{") - line.count("}")
            current_block.append(line)
            if brace_count == 0:
                block_text = "\n".join(current_block)
                try:
                    blocks.append(json.loads(block_text))
                except json.JSONDecodeError:
                    logger.warning("⚠️ Skipping invalid JSON block.")
                inside_json = False
                current_block = []

    return blocks

def flatten_json(obj, key_counter=None, flat_dict=None):
    if flat_dict is None:
        flat_dict = {}
    if key_counter is None:
        key_counter = defaultdict(int)

    def _recurse(item, prefix=""):
        if isinstance(item, dict):
            for key, value in item.items():
                new_key = f"{prefix}{key}" if prefix == "" else f"{prefix} {key}"
                _handle_kv(new_key, value)
        elif isinstance(item, list):
            for i, sub_item in enumerate(item):
                _recurse(sub_item, f"{prefix}[{i}]")

    def _handle_kv(key, value):
        if isinstance(value, dict):
            _recurse(value, prefix=key)
        elif isinstance(value, list):
            flat_value = "\n".join(str(v) for v in value)
            key_counter[key] += 1
            flat_dict[f"{key} {key_counter[key]}"] = flat_value
        else:
            key_counter[key] += 1
            flat_dict[f"{key} {key_counter[key]}"] = str(value)

    _recurse(obj)
    return flat_dict

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

    logger.info(f"📄 Original file content preview:\n{txt_content[:200]}")

    try:
        json_blocks = extract_json_blocks(txt_content)
        if not json_blocks:
            raise ValueError("No valid JSON objects found in file.")

        rows = []
        key_counter = defaultdict(int)
        for obj in json_blocks:
            flat = flatten_json(obj, key_counter=key_counter)
            rows.append(flat)

        all_keys = sorted(set(k for row in rows for k in row.keys()))

        output_stream = BytesIO()
        workbook = xlsxwriter.Workbook(output_stream, {'in_memory': True})
        worksheet = workbook.add_worksheet()

        for col, key in enumerate(all_keys):
            worksheet.write(0, col, key)
        for row_idx, row in enumerate(rows, 1):
            for col_idx, key in enumerate(all_keys):
                worksheet.write(row_idx, col_idx, row.get(key, ""))

        workbook.close()
        output_stream.seek(0)
        full_xlsx = output_stream.read()

    except Exception as e:
        logger.error(f"❌ Failed to flatten and write XLSX: {e}")
        return {"error": f"Flattening or XLSX error: {e}"}

    try:
        basename = input_filename.replace(".txt", "").replace("JSON_input_file_", "")
        if not basename:
            raise ValueError("Empty timestamp extracted from filename.")
        timestamp_str = basename
    except Exception as e:
        logger.warning(f"⚠️ Failed to parse timestamp from input filename: {e}")
        timestamp_str = datetime.utcnow().strftime("%d-%m-%Y_%H-%M-%S")

    output_filename = f"xlsx_output_file_{timestamp_str}.xlsx"
    output_path = f"csv_Output_File/{output_filename}"

    try:
        write_supabase_file(output_path, full_xlsx)
    except Exception as e:
        logger.error(f"❌ Failed to write XLSX file: {e}")
        return {"error": str(e)}

    logger.info(f"✅ XLSX written successfully to: {output_path}")
    return {"status": "success", "csv_path": output_path}

def run_prompt(payload: dict) -> dict:
    return convert_json_to_csv(payload)
