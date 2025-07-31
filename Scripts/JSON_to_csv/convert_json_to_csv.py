import os
import json
import csv
from datetime import datetime
from io import StringIO

from Engine.Files.read_supabase_file import read_supabase_file
from Engine.Files.write_supabase_file import write_supabase_file
from logger import logger


def convert_json_to_csv(payload: dict) -> dict:
    logger.info("🚀 Starting JSON to CSV conversion")

    # 🔹 Step 1: Extract file name from payload
    input_filename = payload.get("file_name")
    if not input_filename:
        logger.error("❌ No file_name provided in payload.")
        return {"error": "file_name not provided"}

    logger.info(f"📥 Input filename: {input_filename}")

    # 🔹 Step 2: Read file content from Supabase
    try:
        txt_content = read_supabase_file(f"JSON_Input_File/{input_filename}")
    except Exception as e:
        logger.error(f"❌ Failed to read input file: {e}")
        return {"error": str(e)}

    logger.info(f"📄 Original file content preview:\n{txt_content[:200]}")

    # 🔹 Step 3: Attempt to parse JSON from content
    try:
        json_data = json.loads(txt_content)
    except json.JSONDecodeError as e:
        logger.error(f"❌ JSON decoding failed: {e}")
        return {"error": f"Invalid JSON: {e}"}

    # 🔹 Step 4: Extract sample key/value from first section
    try:
        first_section = next(iter(json_data.values()))
        first_key = next(iter(first_section))
        first_value = first_section[first_key]
        logger.info(f"🔍 Sample extracted key: {first_key}")
        logger.info(f"🔍 Sample extracted value: {first_value}")
    except Exception as e:
        logger.error(f"❌ Failed to extract sample key/value from JSON: {e}")
        return {"error": f"Unable to extract sample from JSON: {e}"}

    # 🔹 Step 5: Write CSV with extracted sample
    csv_buffer = StringIO()
    csv_writer = csv.writer(csv_buffer)
    csv_writer.writerow([first_key])
    csv_writer.writerow([first_value])
    sample_csv = csv_buffer.getvalue()

    # 🔹 Step 6: Create output file name based on input file date
    basename = input_filename.replace(".txt", "").replace("JSON_input_file_", "")
    try:
        parsed_date = datetime.strptime(basename, "%d-%m-%Y")
        formatted_timestamp = parsed_date.strftime("%Y%m%d")
    except ValueError:
        logger.warning(f"⚠️ Unable to parse date from filename: {basename}. Defaulting to current date.")
        formatted_timestamp = datetime.utcnow().strftime("%Y%m%d")

    output_filename = f"csv_output_file_{formatted_timestamp}.csv"
    output_path = f"csv_Output_File/{output_filename}"

    # 🔹 Step 7: Write to Supabase
    try:
        write_supabase_file(output_path, sample_csv)
    except Exception as e:
        logger.error(f"❌ Failed to write CSV file: {e}")
        return {"error": str(e)}

    logger.info(f"✅ CSV written successfully to: {output_path}")
    return {"status": "success", "csv_path": output_path}


# 🔹 Required by main.py dispatcher
def run_prompt(payload: dict) -> dict:
    return convert_json_to_csv(payload)
