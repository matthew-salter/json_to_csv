import os
import io
import re
import json
from typing import Dict, Any, List, Tuple

import pandas as pd
from supabase import create_client
from logger import logger
from Engine.Files.write_supabase_file import write_supabase_file  # keep existing writer

# -----------------------------------------------------------
# Config
# -----------------------------------------------------------

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_BUCKET = "panelitix"
SUPABASE_ROOT_FOLDER = (os.getenv("SUPABASE_ROOT_FOLDER", "").strip("/"))  # e.g., "JSON_to_csv"

# LIST path is absolute (bucket-rooted); read/write paths are relative to ROOT
INPUT_DIR_REL = "csv_Output_File/"
OUTPUT_DIR_REL = "Formatted_csv_Output_File/"
LIST_DIR_ABS = f"{SUPABASE_ROOT_FOLDER}/{INPUT_DIR_REL}" if SUPABASE_ROOT_FOLDER else INPUT_DIR_REL

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# -----------------------------------------------------------
# Patterns and Output Schema
# -----------------------------------------------------------

SECTION_RE = re.compile(r"^(section)_(.+)_(\d+)$")
SUB_SECTION_RE = re.compile(r"^(sub_section)_(.+)_(\d+\.\d+)$")

OUT_COLUMNS = [
    "report_change",
    "section_number",
    "section_title",
    "section_summary",
    "section_makeup",
    "section_change",
    "section_effect",
    "section_related_article_title",
    "section_related_article_date",
    "section_related_article_summary",
    "section_related_article_relevance",
    "section_related_article_source",
    "sub_section_number",
    "sub_section_title",
    "sub_section_summary",
    "sub_section_makeup",
    "sub_section_change",
    "sub_section_effect",
    "sub_section_related_article_title",
    "sub_section_related_article_date",
    "sub_section_related_article_summary",
    "sub_section_related_article_relevance",
    "sub_section_related_article_source",
]

# -----------------------------------------------------------
# Path helpers
# -----------------------------------------------------------

def _as_rel(path: str) -> str:
    """Ensure a path is RELATIVE to SUPABASE_ROOT_FOLDER (strip if accidentally included)."""
    if not SUPABASE_ROOT_FOLDER:
        return path.lstrip("/")
    prefix = f"{SUPABASE_ROOT_FOLDER}/"
    return path[len(prefix):] if path.startswith(prefix) else path

def _to_abs(rel_path: str) -> str:
    """Make a bucket-rooted ABSOLUTE storage path."""
    rel_path = rel_path.lstrip("/")
    return f"{SUPABASE_ROOT_FOLDER}/{rel_path}" if SUPABASE_ROOT_FOLDER else rel_path

# -----------------------------------------------------------
# Supabase IO (binary-safe)
# -----------------------------------------------------------

def list_folder(abs_prefix: str) -> List[Dict[str, Any]]:
    logger.info(f"📂 Listing: {abs_prefix}")
    return supabase.storage.from_(SUPABASE_BUCKET).list(abs_prefix) or []

def read_xlsx_from_supabase(rel_path: str) -> pd.DataFrame:
    """
    Read Excel as RAW BYTES via Supabase Storage .download (no text decode),
    then parse with pandas/openpyxl.
    """
    rel_path = _as_rel(rel_path)
    abs_path = _to_abs(rel_path)

    logger.info(f"📥 Downloading (abs): {abs_path}")
    raw: bytes = supabase.storage.from_(SUPABASE_BUCKET).download(abs_path)  # returns bytes
    bio = io.BytesIO(raw)

    # Requires openpyxl to be installed
    df = pd.read_excel(bio, engine="openpyxl")
    return df

def write_xlsx_to_supabase(df: pd.DataFrame, rel_path: str) -> None:
    """
    Always write XLSX (binary) using xlsxwriter.
    """
    rel_path = _as_rel(rel_path)
    # Force .xlsx extension (keeps same name if already .xlsx)
    base, _ext = os.path.splitext(rel_path)
    rel_xlsx = f"{base}.xlsx"

    logger.info(f"💾 Writing XLSX (rel): {rel_xlsx}")
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False)
    payload = buf.getvalue()

    write_supabase_file(rel_xlsx, payload)  # helper prefixes SUPABASE_ROOT_FOLDER internally

# -----------------------------------------------------------
# Core Transform
# -----------------------------------------------------------

def transform_flat_sheet_to_long(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert 1-row wide input into long format with:
    - de-suffixed headers
    - section_number (int), sub_section_number (float/NA)
    - repeated section fields each row
    """
    if df.shape[0] == 0:
        return pd.DataFrame(columns=OUT_COLUMNS)

    row = df.iloc[0].to_dict()

    # Gather sections: section_<field>_<N>
    sections: Dict[str, Dict[str, Any]] = {}
    for k, v in row.items():
        m = SECTION_RE.match(k)
        if m:
            _, field, num = m.groups()
            sections.setdefault(num, {})[field] = v

    # Gather sub-sections grouped by parent section: sub_section_<field>_<N.M>
    subsections: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for k, v in row.items():
        m = SUB_SECTION_RE.match(k)
        if m:
            _, field, snum = m.groups()      # e.g., "1.2"
            sec_prefix = snum.split(".")[0]  # "1"
            subsections.setdefault(sec_prefix, {}).setdefault(snum, {})[field] = v

    report_change = row.get("report_change")

    out_rows: List[Dict[str, Any]] = []

    def make_section_base(sec_num: str, sec_fields: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "report_change": report_change,
            "section_number": int(sec_num) if sec_num is not None else None,
            "section_title": sec_fields.get("title"),
            "section_summary": sec_fields.get("summary"),
            "section_makeup": sec_fields.get("makeup"),
            "section_change": sec_fields.get("change"),
            "section_effect": sec_fields.get("effect"),
            "section_related_article_title": sec_fields.get("related_article_title"),
            "section_related_article_date": sec_fields.get("related_article_date"),
            "section_related_article_summary": sec_fields.get("related_article_summary"),
            "section_related_article_relevance": sec_fields.get("related_article_relevance"),
            "section_related_article_source": sec_fields.get("related_article_source"),
        }

    for sec_num in sorted(sections.keys(), key=lambda x: int(x)):
        sec_fields = sections[sec_num]
        sub_map = subsections.get(sec_num, {})

        if sub_map:
            for sub_num in sorted(sub_map.keys(),
                                  key=lambda s: (int(s.split(".")[0]), int(s.split(".")[1]))):
                sub_fields = sub_map[sub_num]
                rec = make_section_base(sec_num, sec_fields)
                rec.update({
                    "sub_section_number": float(sub_num),
                    "sub_section_title": sub_fields.get("title"),
                    "sub_section_summary": sub_fields.get("summary"),
                    "sub_section_makeup": sub_fields.get("makeup"),
                    "sub_section_change": sub_fields.get("change"),
                    "sub_section_effect": sub_fields.get("effect"),
                    "sub_section_related_article_title": sub_fields.get("related_article_title"),
                    "sub_section_related_article_date": sub_fields.get("related_article_date"),
                    "sub_section_related_article_summary": sub_fields.get("related_article_summary"),
                    "sub_section_related_article_relevance": sub_fields.get("related_article_relevance"),
                    "sub_section_related_article_source": sub_fields.get("related_article_source"),
                })
                out_rows.append(rec)
        else:
            rec = make_section_base(sec_num, sec_fields)
            rec.update({
                "sub_section_number": pd.NA,
                "sub_section_title": pd.NA,
                "sub_section_summary": pd.NA,
                "sub_section_makeup": pd.NA,
                "sub_section_change": pd.NA,
                "sub_section_effect": pd.NA,
                "sub_section_related_article_title": pd.NA,
                "sub_section_related_article_date": pd.NA,
                "sub_section_related_article_summary": pd.NA,
                "sub_section_related_article_relevance": pd.NA,
                "sub_section_related_article_source": pd.NA,
            })
            out_rows.append(rec)

    return pd.DataFrame(out_rows, columns=OUT_COLUMNS)

# -----------------------------------------------------------
# Orchestration
# -----------------------------------------------------------

def process_single_file(filename: str) -> str:
    """
    Read one Excel from INPUT_DIR_REL, transform, and write XLSX
    to OUTPUT_DIR_REL with the SAME base filename.
    """
    in_rel = f"{INPUT_DIR_REL}{filename}"
    out_rel = f"{OUTPUT_DIR_REL}{filename}"

    logger.info(f"📥 Reading XLSX (rel): {in_rel}")
    df_in = read_xlsx_from_supabase(in_rel)

    logger.info("🔧 Transforming to formatted long layout...")
    df_out = transform_flat_sheet_to_long(df_in)

    logger.info(f"📤 Writing XLSX (rel): {out_rel}")
    write_xlsx_to_supabase(df_out, out_rel)

    logger.info(f"✅ Done: {out_rel}")
    return out_rel

def process_all_files() -> Dict[str, Any]:
    """Process every file under LIST_DIR_ABS and write formatted XLSX outputs."""
    entries = list_folder(LIST_DIR_ABS)
    written = []

    for e in entries:
        name = e.get("name")
        if not name or name.endswith("/"):
            continue  # skip folders

        # Only process Excel; you said inputs are XLSX
        if not name.lower().endswith((".xlsx", ".xls")):
            logger.info(f"⏭️ Skipping non-Excel file: {name}")
            continue

        try:
            out_path_rel = process_single_file(name)
            written.append(out_path_rel)
        except Exception as ex:
            logger.error(f"❌ Failed to process '{name}': {ex}")

    return {"written": written, "count": len(written)}

def run_prompt(_: dict) -> dict:
    logger.info("🚀 Starting Section/Sub-Section formatter")
    result = process_all_files()
    logger.info(f"🏁 Completed: {result}")
    return result

if __name__ == "__main__":
    print(json.dumps(run_prompt({}), indent=2))
