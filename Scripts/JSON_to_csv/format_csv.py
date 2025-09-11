import os
import io
import re
import json
from typing import Dict, Any, List, Tuple

import pandas as pd
from supabase import create_client
from logger import logger
from Engine.Files.write_supabase_file import write_supabase_file

# -----------------------------------------------------------
# Config
# -----------------------------------------------------------

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_BUCKET = "panelitix"
SUPABASE_ROOT_FOLDER = (os.getenv("SUPABASE_ROOT_FOLDER", "").strip("/"))  # e.g., "JSON_to_csv"

INPUT_DIR_REL = "csv_Output_File/"
OUTPUT_DIR_REL = "Formatted_csv_Output_File/"
LIST_DIR_ABS = f"{SUPABASE_ROOT_FOLDER}/{INPUT_DIR_REL}" if SUPABASE_ROOT_FOLDER else INPUT_DIR_REL

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

try:
    import openpyxl  # noqa: F401
    from openpyxl import __version__ as _oxl_ver
    logger.info(f"✅ openpyxl loaded: v{_oxl_ver}")
except Exception as e:
    logger.error("❌ openpyxl is required to read .xlsx files. Add `openpyxl>=3.1.2` to requirements.")
    raise

# -----------------------------------------------------------
# Output schema
# -----------------------------------------------------------

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
# Helpers
# -----------------------------------------------------------

def _as_rel(path: str) -> str:
    if not SUPABASE_ROOT_FOLDER:
        return path.lstrip("/")
    prefix = f"{SUPABASE_ROOT_FOLDER}/"
    return path[len(prefix):] if path.startswith(prefix) else path

def _to_abs(rel_path: str) -> str:
    rel_path = rel_path.lstrip("/")
    return f"{SUPABASE_ROOT_FOLDER}/{rel_path}" if SUPABASE_ROOT_FOLDER else rel_path

def list_folder(abs_prefix: str) -> List[Dict[str, Any]]:
    logger.info(f"📂 Listing: {abs_prefix}")
    return supabase.storage.from_(SUPABASE_BUCKET).list(abs_prefix) or []

def read_xlsx_from_supabase(rel_path: str) -> pd.DataFrame:
    rel_path = _as_rel(rel_path)
    abs_path = _to_abs(rel_path)
    logger.info(f"📥 Downloading (abs): {abs_path}")
    raw: bytes = supabase.storage.from_(SUPABASE_BUCKET).download(abs_path)
    bio = io.BytesIO(raw)
    df = pd.read_excel(bio, engine="openpyxl")
    logger.info(f"📄 Input sheet shape: {df.shape}. First 5 headers: {list(df.columns)[:5]}")
    return df

def write_xlsx_to_supabase(df: pd.DataFrame, rel_path: str) -> None:
    rel_path = _as_rel(rel_path)
    base, _ext = os.path.splitext(rel_path)
    rel_xlsx = f"{base}.xlsx"
    logger.info(f"💾 Writing XLSX (rel): {rel_xlsx} with shape {df.shape}")
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False)
    write_supabase_file(rel_xlsx, buf.getvalue())

def _canon(s: str) -> str:
    """lower + trim + spaces/dashes -> '_' + collapse multiple '_'"""
    x = (s or "").strip().lower().replace(" ", "_").replace("-", "_")
    x = re.sub(r"_+", "_", x)
    return x

# Accept:
#   section_<field>_<N>
_SECTION_RE = re.compile(r"^section_(?P<field>.+)_(?P<num>\d+)$")

# Accept BOTH:
#   sub_section_<field>_<N>.<M>
#   sub_section_<field>_<N>_<M>
_SUB_RE = re.compile(r"^sub_section_(?P<field>.+)_(?P<snum>\d+(?:[._]\d+))$")

def _normalize_sub_num(snum: str) -> str:
    """Turn '1_2' into '1.2' for consistent float conversion."""
    return snum.replace("_", ".")

# -----------------------------------------------------------
# Core Transform (hardened)
# -----------------------------------------------------------

def transform_flat_sheet_to_long(df: pd.DataFrame) -> pd.DataFrame:
    if df.shape[0] == 0:
        return pd.DataFrame(columns=OUT_COLUMNS)

    # Build canonical key -> value from the first data row
    row0 = df.iloc[0].to_dict()
    canon_row: Dict[str, Any] = {_canon(k): v for k, v in row0.items()}

    # Gather SECTIONS
    sections: Dict[str, Dict[str, Any]] = {}
    for k, v in canon_row.items():
        m = _SECTION_RE.match(k)
        if not m:
            continue
        field = _canon(m.group("field"))
        num = m.group("num")  # keep as string for now
        sections.setdefault(num, {})[field] = v

    # Gather SUB-SECTIONS grouped by section number prefix
    subsections: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for k, v in canon_row.items():
        m = _SUB_RE.match(k)
        if not m:
            continue
        field = _canon(m.group("field"))
        snum_raw = m.group("snum")          # e.g., "1.2" or "1_2"
        snum_norm = _normalize_sub_num(snum_raw)  # "1.2"
        sec_prefix = snum_norm.split(".")[0]      # "1"
        subsections.setdefault(sec_prefix, {}).setdefault(snum_norm, {})[field] = v

    # Fallbacks (in case headers arrived without suffixes — rare, but safe)
    if not sections:
        # detect unsuffixed fields, treat as one section "1"
        hint_keys = [k for k in canon_row.keys() if k.startswith("section_")]
        if hint_keys:
            logger.warning("⚠️ No numbered section headers matched; falling back to single-section mode.")
            sections = {"1": {}}
            for k, v in canon_row.items():
                if k.startswith("section_") and not re.search(r"_\d+$", k):
                    sections["1"][k[len("section_"):]] = v

    report_change = canon_row.get("report_change")
    logger.info(f"🔎 Detected section numbers: {sorted(sections.keys(), key=lambda x: int(x)) if sections else 'NONE'}")
    if subsections:
        _flat_sub_nums = sorted(
            [sn for sec, mp in subsections.items() for sn in mp.keys()],
            key=lambda s: (int(s.split(".")[0]), int(s.split(".")[1]))
        )
        logger.info(f"🔎 Detected sub-section numbers: {_flat_sub_nums}")
    else:
        logger.info("🔎 Detected sub-section numbers: NONE")

    out_rows: List[Dict[str, Any]] = []

    def _base(sec_num: str, sec_fields: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "report_change": report_change,
            "section_number": int(sec_num) if sec_num else None,
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
        sec_fields = sections.get(sec_num, {})
        sub_map = subsections.get(sec_num, {})

        if sub_map:
            for sub_num in sorted(sub_map.keys(),
                                  key=lambda s: (int(s.split(".")[0]), int(s.split(".")[1]))):
                sub_fields = sub_map[sub_num]
                rec = _base(sec_num, sec_fields)
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
            rec = _base(sec_num, sec_fields)
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
    logger.info(f"🧮 Built rows: {len(out_df)}")
    return out_df

# -----------------------------------------------------------
# Orchestration
# -----------------------------------------------------------

def process_single_file(filename: str) -> str:
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
    entries = list_folder(LIST_DIR_ABS)
    written = []
    for e in entries:
        name = e.get("name")
        if not name or name.endswith("/"):
            continue
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
