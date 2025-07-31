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


def flatten_json(obj):
    flat_dict = {}
    section_idx = 0  # e.g., 1, 2, 3
    sub_idx = 0      # e.g., 1.1, 1.2, etc.
    

def flatten_json_structural(json_obj):
    flat_dict = {}
    section_counter = 0

    def recurse(node, prefix="", level=0):
        nonlocal section_counter

        if not isinstance(node, dict):
            return

        local_counter = 0
        for key, value in node.items():
            if isinstance(value, dict):
                if level == 0:
                    section_counter += 1
                    next_prefix = str(section_counter)
                elif level == 1:
                    local_counter += 1
                    next_prefix = f"{prefix}.{local_counter}"
                else:
                    # Ignore anything deeper than 2 levels
                    next_prefix = prefix
                recurse(value, next_prefix, level + 1)
            elif isinstance(value, list):
                joined = "\n".join(str(v) for v in value)
                flat_dict[f"{prefix} {key}"] = joined
            else:
                flat_dict[f"{prefix} {key}"] = str(value) if value is not None else ""

    recurse(json_obj, "", 0)
    return flat_dict
    

def convert_json_to_csv(_: dict) -> dict:
    logger.info("🚀 Starting JSON to CSV conversion")

    # 🔹 Step 1: Locate latest input file
    try:
        input_filename = get_latest_input_file()
    except Exception as e:
        return {"error": str(e)}

    logger.info(f"📥 Input filename selected: {input_filename}")

    # 🔹 Step 2: Read file content from Supabase
    try:
        txt_content = read_supabase_file(f"JSON_Input_File/{input_filename}")
    except Exception as e:
        logger.error(f"❌ Failed to read input file: {e}")
        return {"error": str(e)}

    logger.info(f"📄 Original file content preview:\n{txt_content[:200]}")

    # 🔹 Step 3: Attempt to parse JSON
    try:
        json_data = json.loads(txt_content)
    except json.JSONDecodeError as e:
        logger.error(f"❌ JSON decoding failed: {e}")
        return {"error": f"Invalid JSON: {e}"}

    # 🔹 Step 4: Flatten JSON to asset-title: content pairs
    try:
        flattened = flatten_json(json_data)
        if not flattened:
            raise ValueError("No key-value pairs extracted from JSON.")
        logger.info(f"🔍 Extracted {len(flattened)} asset pairs")
    except Exception as e:
        logger.error(f"❌ Failed to flatten JSON: {e}")
        return {"error": f"Unable to flatten JSON: {e}"}

    # 🔹 Step 5: Write full CSV with headers and content
    csv_buffer = StringIO()
    csv_writer = csv.writer(csv_buffer)
    headers = list(flattened.keys())
    values = [flattened[key] for key in headers]
    csv_writer.writerow(headers)
    csv_writer.writerow(values)
    full_csv = csv_buffer.getvalue()

    # 🔹 Step 6: Timestamp from input filename
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

    # 🔹 Step 7: Write to Supabase
    try:
        write_supabase_file(output_path, full_csv)
    except Exception as e:
        logger.error(f"❌ Failed to write CSV file: {e}")
        return {"error": str(e)}

    logger.info(f"✅ CSV written successfully to: {output_path}")
    return {"status": "success", "csv_path": output_path}


# 🔹 Required by main.py dispatcher
def run_prompt(payload: dict) -> dict:
    return convert_json_to_csv(payload)
