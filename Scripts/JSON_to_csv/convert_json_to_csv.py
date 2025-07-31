import os
from Engine.Files.read_supabase_file import read_supabase_file
from Engine.Files.write_supabase_file import write_supabase_file
from datetime import datetime
import csv
from io import StringIO
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

    # 🔹 Step 3: Create dummy CSV content
    csv_buffer = StringIO()
    csv_writer = csv.writer(csv_buffer)
    csv_writer.writerow(["Header 1", "Header 2"])
    csv_writer.writerow(["hello", "world"])
    dummy_csv = csv_buffer.getvalue()

    # 🔹 Step 4: Write CSV back to Supabase
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_filename = input_filename.replace(".txt", f"_{timestamp}.csv")
    output_path = f"csv_Output_File/{output_filename}"

    try:
        write_supabase_file(output_path, dummy_csv)
    except Exception as e:
        logger.error(f"❌ Failed to write CSV file: {e}")
        return {"error": str(e)}

    logger.info(f"✅ CSV written successfully to: {output_path}")
    return {"status": "success", "csv_path": output_path}

# 🔹 Required by main.py dispatcher
def run_prompt(payload: dict) -> dict:
    return convert_json_to_csv(payload)
