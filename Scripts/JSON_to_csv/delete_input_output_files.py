import os
import json
import requests
from typing import Dict, List, Tuple

from Engine.Files.auth import get_supabase_headers
from logger import logger

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_BUCKET = "panelitix"
SUPABASE_ROOT_FOLDER = os.getenv("SUPABASE_ROOT_FOLDER", "JSON_to_csv")

INPUT_FOLDER = f"{SUPABASE_ROOT_FOLDER}/JSON_Input_File"
OUTPUT_FOLDER = f"{SUPABASE_ROOT_FOLDER}/csv_Output_File"
FORMATTED_OUTPUT_FOLDER = f"{SUPABASE_ROOT_FOLDER}/Formatted_csv_Output_File"


def _list_page(folder: str, limit: int = 100, offset: int = 0) -> List[dict]:
    """
    Returns a page of entries under `folder` from:
      POST /storage/v1/object/list/{bucket}
    Each entry is a dict with at least keys: "name", "id", "metadata" (files have metadata including size).
    Folders typically return metadata=None.
    """
    if not SUPABASE_URL:
        raise RuntimeError("SUPABASE_URL not configured")

    url = f"{SUPABASE_URL}/storage/v1/object/list/{SUPABASE_BUCKET}"
    headers = get_supabase_headers()
    headers["Content-Type"] = "application/json"

    payload = {
        "prefix": folder.rstrip("/"),
        "limit": limit,
        "offset": offset,
        "sortBy": {"column": "name", "order": "asc"},
    }

    logger.info(f"📄 Listing (limit={limit}, offset={offset}) under {SUPABASE_BUCKET}/{folder}")
    resp = requests.post(url, headers=headers, data=json.dumps(payload))
    logger.debug(f"🛰️ List status: {resp.status_code}, body(sample): {resp.text[:300]}")
    resp.raise_for_status()
    items = resp.json() or []
    # Defensive: ensure dict entries only
    return [it for it in items if isinstance(it, dict) and it.get("name") is not None]


def _list_all(folder: str, page_size: int = 100) -> List[dict]:
    """
    Paginate until all entries are fetched.
    """
    entries: List[dict] = []
    offset = 0
    while True:
        page = _list_page(folder, limit=page_size, offset=offset)
        if not page:
            break
        entries.extend(page)
        if len(page) < page_size:
            break
        offset += page_size
    return entries


def _is_file(entry: dict) -> bool:
    """
    Heuristic based on Supabase response:
    - Files have `metadata` (with size, mimetype, etc.)
    - Folders typically have metadata == None
    """
    md = entry.get("metadata")
    return isinstance(md, dict)


def _delete_objects(paths: List[str]) -> None:
    """
    DELETE each object individually:
      DELETE /storage/v1/object/{bucket}/{objectPath}
    Only pass FILE paths here (never folders).
    """
    if not paths:
        return
    if not SUPABASE_URL:
        raise RuntimeError("SUPABASE_URL not configured")

    headers = get_supabase_headers()

    for p in paths:
        # Safety: never try to delete a directory marker
        if p.endswith("/"):
            logger.info(f"🚫 Skipping folder-like path (won't delete folders): {p}")
            continue
        url = f"{SUPABASE_URL}/storage/v1/object/{SUPABASE_BUCKET}/{p}"
        logger.info(f"🗑️ Deleting file: {SUPABASE_BUCKET}/{p}")
        resp = requests.delete(url, headers=headers)
        if resp.status_code not in (200, 204):
            logger.debug(f"🛰️ Delete status: {resp.status_code}, body: {resp.text[:300]}")
            resp.raise_for_status()


def _empty_folder_recursive(folder: str) -> Dict[str, int]:
    """
    Recursively delete all files under `folder` (and its subfolders), but never delete the folder itself.
    """
    total_deleted = 0
    # Normalize folder (no trailing slash)
    folder = folder.rstrip("/")

    # Paginated list of current level
    entries = _list_all(folder)

    # Separate files and subfolders
    file_paths: List[str] = []
    subfolders: List[str] = []

    for it in entries:
        name = it["name"]
        if _is_file(it):
            # File path relative to bucket
            file_paths.append(f"{folder}/{name}")
        else:
            # Likely a subfolder inside `folder`
            # Supabase returns just the immediate child name (e.g., "subdir")
            subfolders.append(f"{folder}/{name}")

    # Delete files at this level
    _delete_objects(file_paths)
    total_deleted += len(file_paths)

    # Recurse into subfolders
    for sub in subfolders:
        # Ensure no trailing slash doubling
        sub = sub.rstrip("/")
        deleted_in_sub = _empty_folder_recursive(sub)["deleted"]
        total_deleted += deleted_in_sub

    logger.info(f"✅ Emptied files under {SUPABASE_BUCKET}/{folder} (deleted {total_deleted})")
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
    logger.info(f"   Formatted Output: {FORMATTED_OUTPUT_FOLDER}")

    results = {
        INPUT_FOLDER: _empty_folder_recursive(INPUT_FOLDER),
        OUTPUT_FOLDER: _empty_folder_recursive(OUTPUT_FOLDER),
        FORMATTED_OUTPUT_FOLDER: _empty_folder_recursive(FORMATTED_OUTPUT_FOLDER),
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
