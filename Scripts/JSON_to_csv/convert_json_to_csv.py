import os
import json
import csv
from datetime import datetime
from io import StringIO
from collections import defaultdict
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

def split_multiple_jsons(text):
    snippets = []
    buffer = []
    in_json = False
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("{"):
            buffer = [stripped]
            in_json = True
        elif stripped.endswith("}") and in_json:
            buffer.append(stripped)
            joined = "\n".join(buffer)
            try:
                snippets.append(json.loads(joined))
            except json.JSONDecodeError:
                logger.warning("⚠️ Skipping invalid JSON block.")
            buffer = []
            in_json = False
        elif in_json:
            buffer.append(stripped)
    return snippets

def flatten_json(obj, key_counter=None):
    flat_dict = {}
    key_counter = key_counter or defaultdict(int)

    def _recurse(item):
        if isinstance(item, dict):
            for key, value in item.items():
                _handle_kv(key, value)
        elif isinstance(item, list):
            for sub_item in item:
                _recurse(sub_item)

    def _handle_kv(key, value):
        if isinstance(value, dict):
            _recurse(value)
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
    logger.info("🚀 Starting JSON to CSV conversion")

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
        json_objects = split_multiple_jsons(txt_content)
        if not json_objects:
            raise ValueError("No valid JSON objects found in file.")
    except Exception as e:
        logger.error(f"❌ Failed to parse multiple JSON objects: {e}")
        return {"error": f"Invalid JSON structure: {e}"}

    try:
        rows = []
        key_counter = defaultdict(int)
        for obj in json_objects:
            flat = flatten_json(obj, key_counter)
            rows.append(flat)

        all_keys = sorted(set(k for row in rows for k in row.keys()))
        csv_buffer = StringIO(newline="")
        csv_writer = csv.writer(csv_buffer)
        csv_writer.writerow(all_keys)
        for row in rows:
            csv_writer.writerow([row.get(k, "") for k in all_keys])

        full_csv = csv_buffer.getvalue()
    except Exception as e:
        logger.error(f"❌ Failed to flatten and write CSV: {e}")
        return {"error": f"Flattening or CSV error: {e}"}

    try:
        basename = input_filename.replace(".txt", "").replace("JSON_input_file_", "")
        if not basename:
            raise ValueError("Empty timestamp extracted from filename.")
        timestamp_str = basename
    except Exception as e:
        logger.warning(f"⚠️ Failed to parse timestamp from input filename: {e}")
        timestamp_str = datetime.utcnow().strftime("%d-%m-%Y_%H-%M-%S")

    output_filename = f"csv_output_file_{timestamp_str}.csv"
    output_path = f"csv_Output_File/{output_filename}"

    try:
        write_supabase_file(output_path, full_csv.encode("utf-8"))
    except Exception as e:
        logger.error(f"❌ Failed to write CSV file: {e}")
        return {"error": str(e)}

    logger.info(f"✅ CSV written successfully to: {output_path}")
    return {"status": "success", "csv_path": output_path}

def run_prompt(payload: dict) -> dict:
    return convert_json_to_csv(payload)
