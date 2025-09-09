import os
import io
import re
import json
from typing import Dict, Any, List, Tuple

import pandas as pd
from supabase import create_client
from logger import logger
from Engine.Files.read_supabase_file import read_supabase_file
from Engine.Files.write_supabase_file import write_supabase_file

# -----------------------------------------------------------
# Config
# -----------------------------------------------------------

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_BUCKET = "panelitix"

INPUT_DIR = "JSON_to_csv/csv_Output_File/"
OUTPUT_DIR = "JSON_to_csv/Formatted_csv_Output_File/"

# Keep filename (incl. extension) identical between input and output
PRESERVE_EXTENSION = True

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
# Supabase helpers
# -----------------------------------------------------------

def list_folder(prefix: str) -> List[Dict[str, Any]]:
    """List objects under a folder in Supabase Storage."""
    logger.info(f"📂 Listing: {prefix}")
    try:
        items = supabase.storage.from_(SUPABASE_BUCKET).list(prefix)
        return items or []
    except Exception as e:
        logger.error(f"❌ Failed to list Supabase folder '{prefix}': {e}")
        raise

def read_sheet_from_supabase(path: str) -> Tuple[pd.DataFrame, str]:
    """Read a sheet (xlsx/csv) from Supabase path -> DataFrame and extension."""
    try:
        raw = read_supabase_file(path)  # returns bytes
    except Exception as e:
        logger.error(f"❌ Failed to read '{path}': {e}")
        raise

    name = os.path.basename(path).lower()
    bio = io.BytesIO(raw)

    if name.endswith(".xlsx") or name.endswith(".xls"):
        df = pd.read_excel(bio)
        ext = ".xlsx"
    elif name.endswith(".csv") or name.endswith(".txt") or name.endswith(".tsv"):
        # Fallback to CSV parsing
        df = pd.read_csv(bio)
        # Normalize: ensure a single-row wide format
        ext = ".csv"
    else:
        # Default to Excel if unsure
        df = pd.read_excel(bio)
        ext = ".xlsx"

    return df, ext

def write_sheet_to_supabase(df: pd.DataFrame, out_path: str) -> None:
    """Write DataFrame to Supabase (xlsx if .xlsx; csv if .csv/.txt)."""
    logger.info(f"💾 Writing formatted file: {out_path}")
    ext = os.path.splitext(out_path)[1].lower()

    if ext in [".csv", ".txt", ".tsv"]:
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        payload = buf.getvalue().encode("utf-8")
    else:
        # Default to Excel
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False)
        payload = buf.getvalue()

    try:
        write_supabase_file(out_path, payload)
    except Exception as e:
        logger.error(f"❌ Failed to write '{out_path}': {e}")
        raise

# -----------------------------------------------------------
# Core Transform
# -----------------------------------------------------------

def transform_flat_sheet_to_long(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the one-row 'wide' sheet into the 'long' formatted sheet.

    - De-suffix headers (keep unique titles without numbering)
    - Add 'section_number' (int) and 'sub_section_number' (float)
    - Repeat section-level fields on each row
    - Place sub-section fields row-by-row
    """
    if df.shape[0] == 0:
        return pd.DataFrame(columns=OUT_COLUMNS)

    # The input example has a single row with all values
    row = df.iloc[0].to_dict()

    # Gather sections: section_<field>_<N>
    sections: Dict[str, Dict[str, Any]] = {}
    for k, v in row.items():
        m = SECTION_RE.match(k)
        if m:
            _, field, num = m.groups()  # field like 'title', 'summary', etc.
            sections.setdefault(num, {})[field] = v

    # Gather sub-sections grouped by parent section: sub_section_<field>_<N.M>
    subsections: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for k, v in row.items():
        m = SUB_SECTION_RE.match(k)
        if m:
            _, field, snum = m.groups()     # snum e.g. "1.2"
            sec_prefix = snum.split(".")[0] # "1"
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

    # Build rows in numeric order of section and sub-section
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
            # Section without sub-sections → one row with NA sub-section fields
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

    out_df = pd.DataFrame(out_rows, columns=OUT_COLUMNS)
    return out_df

# -----------------------------------------------------------
# Orchestration
# -----------------------------------------------------------

def process_single_file(filename: str) -> str:
    """Read one input sheet, transform, and write to output with SAME filename."""
    in_path = f"{INPUT_DIR}{filename}"
    out_path = f"{OUTPUT_DIR}{filename}"

    logger.info(f"📥 Reading: {in_path}")
    df_in, _ext = read_sheet_from_supabase(in_path)

    logger.info("🔧 Transforming to formatted long layout...")
    df_out = transform_flat_sheet_to_long(df_in)

    logger.info(f"📤 Writing: {out_path}")
    write_sheet_to_supabase(df_out, out_path)

    logger.info(f"✅ Done: {out_path}")
    return out_path

def process_all_files() -> Dict[str, Any]:
    """Process every file under INPUT_DIR and write formatted outputs."""
    entries = list_folder(INPUT_DIR)
    written = []

    for e in entries:
        name = e.get("name")
        if not name or name.endswith("/"):
            continue  # skip folders
        # Skip non-sheet files
        lname = name.lower()
        if not (lname.endswith(".xlsx") or lname.endswith(".xls")
                or lname.endswith(".csv") or lname.endswith(".txt") or lname.endswith(".tsv")):
            continue
        try:
            out_path = process_single_file(name)
            written.append(out_path)
        except Exception as ex:
            logger.error(f"❌ Failed to process '{name}': {ex}")

    return {"written": written, "count": len(written)}

# Optional: if you want a callable entry-point for your Zapier/runner
def run_prompt(_: dict) -> dict:
    logger.info("🚀 Starting Section/Sub-Section formatter")
    result = process_all_files()
    logger.info(f"🏁 Completed: {result}")
    return result

if __name__ == "__main__":
    print(json.dumps(run_prompt({}), indent=2))
