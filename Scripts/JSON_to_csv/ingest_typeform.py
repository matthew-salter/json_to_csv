import os
import requests
import time
from datetime import datetime
from logger import logger
from pathlib import Path
from Engine.Files.write_supabase_file import write_supabase_file

# --- ENV VARS ---
json_file_field_id = os.getenv("JSON_FILE_FIELD_ID")
SUPABASE_ROOT_FOLDER = os.getenv("SUPABASE_ROOT_FOLDER")
SUPABASE_URL = os.getenv("SUPABASE_URL")

logger.info("🌍 ENV VARS (ingest_typeform.py):")
logger.info(f"   JSON_FILE_FIELD_ID = {json_file_field_id}")
logger.info(f"   SUPABASE_ROOT_FOLDER = {SUPABASE_ROOT_FOLDER}")
logger.info(f"   SUPABASE_URL = {SUPABASE_URL}")

# --- HELPERS ---
def download_file(url: str, retries: int = 3, delay: int = 2) -> bytes:
    """Downloads a file from a given URL and returns its binary content, with Typeform auth if needed."""
    headers = {}

    if "api.typeform.com/responses/files" in url:
        typeform_token = os.getenv("TYPEFORM_TOKEN")
        if not typeform_token:
            raise EnvironmentError("TYPEFORM_TOKEN not set in environment variables")
        headers["Authorization"] = f"Bearer {typeform_token}"

    for attempt in range(1, retries + 1):
        logger.info(f"🌐 Attempting download (try {attempt}) from URL: {url}")
        try:
            res = requests.get(url, headers=headers, timeout=10)
            if res.status_code != 200:
                logger.warning(f"📡 HTTP {res.status_code} - Response headers: {res.headers}")
                logger.warning(f"📡 Response content preview: {res.content[:200]}")
            res.raise_for_status()
            logger.info(f"📥 Download successful (size = {len(res.content)} bytes)")
            return res.content
        except requests.RequestException as e:
            logger.warning(f"⚠️ Download failed (attempt {attempt}): {e}")
            if attempt < retries:
                time.sleep(delay)
            else:
                logger.error(f"❌ Failed to download file after {retries} attempts: {url}")
                raise

# --- MAIN FUNCTION ---
def process_typeform_submission(data):
    """Extracts JSON file from Typeform and writes it to Supabase."""
    try:
        answers = data.get("form_response", {}).get("answers", [])
        submitted_at = data.get("form_response", {}).get("submitted_at")
        if submitted_at:
            logger.info(f"🕒 Typeform submitted_at: {submitted_at}")
        logger.info(f"🕒 Script start time: {datetime.utcnow().isoformat()}")

        json_file_url = None

        logger.info("📦 Parsing Typeform answers...")
        for answer in answers:
            field_id = answer["field"]["id"]
            logger.debug(f"🧩 Field ID: {field_id}, Type: {answer['type']}")

            if field_id == json_file_field_id:
                json_file_url = answer["file_url"]
                logger.info(f"📄 JSON input file URL: {json_file_url}")

        if not json_file_url:
            raise ValueError("❌ Missing required field: JSON file URL")

        # Build Supabase path
        date_str = datetime.utcnow().strftime("%d-%m-%Y")
        file_path = f"JSON_Input_File/JSON_input_file_{date_str}.txt"
        logger.info(f"🧾 Final Supabase path: {file_path}")

        # Download file
        logger.info(f"⬇️ Downloading JSON input file from: {json_file_url}")
        file_data = download_file(json_file_url)

        # Decode and log preview
        try:
            decoded_file = file_data.decode("utf-8")
            logger.info("✅ Decoded JSON file as UTF-8 successfully")
        except UnicodeDecodeError as e:
            logger.error(f"❌ Failed to decode file as UTF-8: {e}")
            raise

        logger.info(f"🔎 File content preview (first 100 chars): {repr(decoded_file[:100])}")
        logger.info(f"📏 File length (chars): {len(decoded_file)}")

        # Save to Supabase
        write_supabase_file(file_path, decoded_file)
        logger.info("✅ JSON file written to Supabase successfully.")

    except Exception:
        logger.exception("❌ Failed to process Typeform submission and save file to Supabase.")
