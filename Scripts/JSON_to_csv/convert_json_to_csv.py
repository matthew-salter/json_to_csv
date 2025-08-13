import os
import re
import json
from datetime import datetime
from io import BytesIO
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
    response = supabase.storage.from_(SUPABASE_BUCKET).list(SUPABASE_FOLDER)
    txt_files = [f["name"] for f in response if f["name"].endswith(".txt")]
    if not txt_files:
        raise FileNotFoundError("No .txt files found in input folder.")
    txt_files.sort(reverse=True)
    latest = txt_files[0]
    logger.info(f"🕒 Latest input file detected: {latest}")
    return latest

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
        if re.match(r'^"\s*[^"]+\s*"\s*:\s*".*?"\s*,?$', stripped):
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

def flatten_json(obj):
    flat_dict = {}

    def format_key(k):
        return k.strip().lower().replace(" ", "_").replace("-", "_")

    def clean_value(v):
        return str(v).replace("\n", "\\n") if isinstance(v, str) else str(v)

    def recurse(d):
        if isinstance(d, dict):
            for key, value in d.items():
                f_key = format_key(key)
                if isinstance(value, dict):
                    recurse(value)
                elif isinstance(value, list):
                    flat_dict[f_key] = "\\n".join(
                        str(v).replace("\n", "\\n") if isinstance(v, str) else str(v)
                        for v in value
                    )
                else:
                    flat_dict[f_key] = clean_value(value)
        elif isinstance(d, list):
            for item in d:
                recurse(item)

    recurse(obj)
    return flat_dict

def process_json_objects(json_objects):
    merged_flat = {}
    for obj in json_objects:
        flat = flatten_json(obj)
        merged_flat.update(flat)
    return [merged_flat]

def convert_json_to_csv(_: dict) -> dict:
    logger.info("🚀 Starting JSON to XLSX conversion")
    input_filename = get_latest_input_file()
    logger.info(f"📥 Input filename selected: {input_filename}")
    txt_content = read_supabase_file(f"JSON_Input_File/{input_filename}")
    logger.info(f"📄 File content preview:\n{txt_content[:400]}")

    json_objects = split_multiple_jsons(txt_content)
    if not json_objects:
        raise ValueError("No valid JSON objects found in file.")

    rows = process_json_objects(json_objects)
    ordered_keys = list(rows[0].keys())

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

    basename = input_filename.replace(".txt", "").replace("JSON_input_file_", "")
    timestamp_str = basename if basename else datetime.utcnow().strftime("%d-%m-%Y_%H-%M-%S")
    output_filename = f"xlsx_output_file_{timestamp_str}.xlsx"
    output_path = f"csv_Output_File/{output_filename}"

    write_supabase_file(output_path, full_xlsx)
    logger.info(f"✅ XLSX written to: {output_path}")
    return {"status": "success", "csv_path": output_path}

def run_prompt(payload: dict) -> dict:
    return convert_json_to_csv(payload)
