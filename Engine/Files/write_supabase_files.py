import os
import requests
from Engine.Files.auth import get_supabase_headers
from logger import logger

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_BUCKET = "panelitix"
SUPABASE_ROOT_FOLDER = os.getenv("SUPABASE_ROOT_FOLDER")

logger.info("🌍 ENV VARS (write_supabase_file.py):")
logger.info(f"   SUPABASE_URL = {SUPABASE_URL}")
logger.info(f"   SUPABASE_BUCKET = {SUPABASE_BUCKET}")
logger.info(f"   SUPABASE_ROOT_FOLDER = {SUPABASE_ROOT_FOLDER}")

def write_supabase_file(path, content, content_type=None):
    if not SUPABASE_URL:
        logger.error("❌ SUPABASE_URL is not set in environment variables.")
        raise ValueError("SUPABASE_URL not configured")

    if not SUPABASE_ROOT_FOLDER:
        logger.error("❌ SUPABASE_ROOT_FOLDER is not set in environment variables.")
        raise ValueError("SUPABASE_ROOT_FOLDER not configured")

    if not path:
        logger.error("❌ No path provided to write_supabase_file")
        raise ValueError("File path must be provided")

    # 🔹 Compose full Supabase path
    full_path = f"{SUPABASE_ROOT_FOLDER}/{path}"
    url = f"{SUPABASE_URL}/storage/v1/object/{SUPABASE_BUCKET}/{full_path}"

    logger.info("📁 Supabase Write Operation Initiated:")
    logger.info(f"   → Relative Path: {path}")
    logger.info(f"   → Full Path: {full_path}")
    logger.info(f"   → Target URL: {url}")

    headers = get_supabase_headers()

    # --- Encode content and log preview ---
    if isinstance(content, str):
        try:
            data = content.encode("utf-8", errors="strict")
            logger.debug("📄 Content is valid UTF-8 string. Encoding before upload.")
            logger.debug(f"🔍 Preview of encoded string content (first 100 bytes): {data[:100]}")
        except UnicodeEncodeError as e:
            logger.error(f"❌ UTF-8 encoding failed: {e}")
            raise
    elif isinstance(content, bytes):
        data = content
        logger.debug("🖼️ Content is raw bytes. Uploading directly.")
        logger.debug(f"🔍 Preview of byte content (first 100 bytes): {data[:100]}")
    else:
        logger.error("❌ Content must be either str or bytes.")
        raise TypeError("Content must be str or bytes")

    logger.info(f"📏 Upload size: {len(data)} bytes")

    # --- Determine Content-Type ---
    if content_type:
        headers["Content-Type"] = content_type
        logger.debug(f"🧾 Custom Content-Type provided: {content_type}")
    elif path.endswith(".csv"):
        headers["Content-Type"] = "text/csv; charset=utf-8"
        logger.debug("🧾 CSV file detected. Using Content-Type: text/csv")
    elif path.endswith(".txt"):
        headers["Content-Type"] = "text/plain; charset=utf-8"
        logger.debug("📑 TXT file detected. Using Content-Type: text/plain")
    else:
        headers["Content-Type"] = "application/octet-stream"
        logger.debug("📦 Unknown file type. Defaulting to application/octet-stream")

    logger.debug(f"📦 Final headers: {headers}")

    # --- Upload to Supabase ---
    try:
        logger.info(f"🚀 Initiating PUT request to Supabase at: {url}")
        response = requests.put(url, headers=headers, data=data)

        logger.info(f"📡 Supabase response status: {response.status_code}")
        logger.debug(f"📨 Supabase raw response: {response.text}")

        response.raise_for_status()

        # Final check
        try:
            returned_key = response.json().get("Key")
            logger.info(f"🔑 Supabase confirmed object key: {returned_key}")
        except Exception as parse_err:
            logger.warning("⚠️ Unable to parse JSON response from Supabase.")

        logger.info(f"✅ File successfully written to Supabase at: {full_path}")

    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Supabase write failed: {e}")
        raise
