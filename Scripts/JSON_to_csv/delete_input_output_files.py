import os
import json
import requests
from typing import Dict, List

from Engine.Files.auth import get_supabase_headers
from logger import logger

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_BUCKET = "panelitix"
SUPABASE_ROOT_FOLDER = os.getenv("SUPABASE_ROOT_FOLDER", "JSON_to_csv")

INPUT_FOLDER = f"{SUPABASE_ROOT_FOLDER}/JSON_Input_File"
OUTPUT_FOLDER = f"{SUPABASE_ROOT_FOLDER}/csv_Output_File"


def _list_objects(folder: str, limit: int = 100) -> List[str]:
    """
    Returns a list of object names (not full paths) under `folder`.
    Uses: POST /storage/v1/object/list/{bucket}
    """
    if not SUPABASE_URL:
        raise RuntimeError("SUPABASE_URL not configured")

    url = f"{SUPABASE_URL}/storage/v1/object/list/{SUPABASE_BUCKET}"
    headers = get_supabase_headers()
    headers["Content-Type"] = "application/json"

    payload = {
        "prefix": folder,
        "limit": limit,
        "offset": 0,
        "sortBy": {"column": "name", "order": "asc"},
    }

    logger.info(f"📄 Listing objects under {SUPABASE_BUCKET}/{folder}")
    resp = requests.post(url, headers=headers, data=json.dumps(payload))
    logger.debug(f"🛰️ List status: {resp.status_code}, body: {resp.text[:300]}")
    resp.raise_for_status()

    items = resp.json() or []
    # The API returns objects at that prefix; each item has "name"
    names = [it["name"] for it in items if isinstance(it, dict) and it.get("name")]
    return names


def _delete_paths(paths: List[str]) -> None:
    """
    Batch delete by full paths relative to the bucket.
    Uses: POST /storage/v1/object/remove with {"prefixes": ["folder/file1", ...]}
    """
    if not paths:
        return

    url = f"{SUPABASE_URL}/storage/v1/object/remove"
    headers = get_supabase_headers()
    headers["Content-Type"] = "application/json"

    payload = {"prefixes": paths}
    logger.info(f"🗑️ Deleting {len(paths)} object(s)")
    resp = requests.post(url, headers=headers, data=json.dumps(payload))
    logger.debug(f"🛰️ Remove status: {resp.status_code}, body: {resp.text[:300]}")
    resp.raise_for_status()


def _empty_folder(folder: str) -> Dict[str, int]:
    """
    Re-list and batch delete until the folder is empty.
    """
    total_deleted = 0
    while True:
        names = _list_objects(folder, limit=100)
        if not names:
            break
        # Build full paths relative to bucket
        full_paths = [f"{folder}/{name}" for name in names]
        _delete_paths(full_paths)
        total_deleted += len(full_paths)

    logger.info(f"✅ Emptied {SUPABASE_BUCKET}/{folder} (deleted {total_deleted})")
    return {"deleted": total_deleted}


def delete_input_output_files() -> Dict[str, Dict[str, int]]:
    if not SUPABASE_URL:
        raise RuntimeError("SUPABASE_URL not configured")
    if not SUPABASE_ROOT_FOLDER:
        raise RuntimeError("SUPABASE_ROOT_FOLDER not configured")

    logger.info("🧹 Starting Supabase cleanup for JSON-to-CSV folders")
    logger.info(f"   Bucket: {SUPABASE_BUCKET}")
    logger.info(f"   Input:  {INPUT_FOLDER}")
    logger.info(f"   Output: {OUTPUT_FOLDER}")

    results = {
        INPUT_FOLDER: _empty_folder(INPUT_FOLDER),
        OUTPUT_FOLDER: _empty_folder(OUTPUT_FOLDER),
    }
    logger.info(f"🧾 Cleanup results: {results}")
    return results

def run_prompt(payload: dict | None = None):
    """
    Adapter for main.py's dispatcher.
    Ignores the payload for now and runs the cleanup.
    Returns a JSON-serializable dict.
    """
    try:
        results = delete_input_output_files()
        return {"status": "ok", "results": results}
    except Exception as e:
        logger.exception("❌ Cleanup failed in run_prompt()")
        return {"status": "error", "error": str(e)}

if __name__ == "__main__":
    try:
        out = delete_input_output_files()
        print(json.dumps({"bucket": SUPABASE_BUCKET, "results": out}, indent=2))
    except Exception as e:
        logger.exception("❌ Cleanup failed")
        print(json.dumps({"error": str(e)}))
        raise
