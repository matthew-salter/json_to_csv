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
except Exception:
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
    x = (s or "").strip().lower().replace(" ", "_").replace("-", "_")
    return re.sub(r"_+", "_", x)

# Accept: section_<field>_<N>
_SECTION_RE = re.compile(r"^section_(?P<field>.+)_(?P<num>\d+)$")
# Accept both: sub_section_<field>_<N>.<M> and sub_section_<field>_<N>_<M>
_SUB_RE = re.compile(r"^sub_section_(?P<field>.+)_(?P<snum>\d+(?:[._]\d+))$")

def _normalize_sub_num(snum: str) -> str:
    return snum.replace("_", ".")

def _has_section_schema(canon_cols: List[str]) -> bool:
    return any(_SECTION_RE.match(c) for c in canon_cols) or any(_SUB_RE.match(c) for c in canon_cols)

# -----------------------------------------------------------
# Core Transform
# -----------------------------------------------------------

def transform_flat_sheet_to_long(df: pd.DataFrame) -> pd.DataFrame:
    if df.shape[0] == 0:
        return pd.DataFrame(columns=OUT_COLUMNS)

    # Canonicalize headers and row0 values
    row0 = df.iloc[0].to_dict()
    canon_row: Dict[str, Any] = {_canon(k): v for k, v in row0.items()}
    canon_cols = list(canon_row.keys())

    # Schema check
    if not _has_section_schema(canon_cols):
        logger.warning("⚠️ No section/sub-section headers detected. "
                       "Expected e.g. 'section_title_1', 'sub_section_title_1.1'. "
                       f"First 20 canonical headers: {canon_cols[:20]}")
        # Return empty to signal caller to SKIP WRITING
        return pd.DataFrame(columns=OUT_COLUMNS)

    # Gather sections
    sections: Dict[str, Dict[str, Any]] = {}
    for k, v in canon_row.items():
        m = _SECTION_RE.match(k)
        if not m:
            continue
        field = _canon(m.group("field"))
        num = m.group("num")
        sections.setdefault(num, {})[field] = v

    # Gather sub-sections
    subsections: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for k, v in canon_row.items():
        m = _SUB_RE.match(k)
        if not m:
            continue
        field = _canon(m.group("field"))
        snum_norm = _normalize_sub_num(m.group("snum"))  # "1.2"
        sec_prefix = snum_norm.split(".")[0]             # "1"
        subsections.setdefault(sec_prefix, {}).setdefault(snum_norm, {})[field] = v

    report_change = canon_row.get("report_change")

    if sections:
        logger.info(f"🔎 Detected section numbers: {sorted(sections.keys(), key=lambda x: int(x))}")
    else:
        logger.info("🔎 Detected section numbers: NONE")

    if subsections:
        flat_subs = sorted(
            [sn for sec, mp in subsections.items() for sn in mp.keys()],
            key=lambda s: (int(s.split(".")[0]), int(s.split(".")[1]))
        )
        logger.info(f"🔎 Detected sub-section numbers: {flat_subs}")
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
            for sub_num in sorted(
                sub_map.keys(), key=lambda s: (int(s.split(".")[0]), int(s.split(".")[1]))
            ):
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

def process_single_file(filename: str) -> Tuple[str, bool, str]:
    """
    Returns: (name, written_bool, reason_if_skipped)
    """
    in_rel = f"{INPUT_DIR_REL}{filename}"
    out_rel = f"{OUTPUT_DIR_REL}{filename}"

    logger.info(f"📥 Reading XLSX (rel): {in_rel}")
    df_in = read_xlsx_from_supabase(in_rel)

    logger.info("🔧 Transforming to formatted long layout...")
    df_out = transform_flat_sheet_to_long(df_in)

    # If no rows, skip writing and explain why
    if df_out.shape[0] == 0:
        logger.warning(f"⏭️ Skipping write for '{filename}': no rows built (schema mismatch).")
        return (filename, False, "no section/sub-section headers found")

    logger.info(f"📤 Writing XLSX (rel): {out_rel}")
    write_xlsx_to_supabase(df_out, out_rel)

    logger.info(f"✅ Done: {out_rel}")
    return (filename, True, "")

def process_all_files(debug_headers: bool = False, header_preview: int = 20) -> Dict[str, Any]:
    entries = list_folder(LIST_DIR_ABS)
    written, skipped = [], []

    for e in entries:
        name = e.get("name")
        if not name or name.endswith("/"):
            continue
        if not name.lower().endswith((".xlsx", ".xls")):
            logger.info(f"⏭️ Skipping non-Excel file: {name}")
            continue

        if debug_headers:
            # dump first N canonical headers then continue (no write)
            in_rel = f"{INPUT_DIR_REL}{name}"
            df = read_xlsx_from_supabase(in_rel)
            canon_cols = [_canon(c) for c in list(df.columns)]
            logger.info(f"🪪 {name} — first {header_preview} headers: {canon_cols[:header_preview]}")
            continue

        fname, did_write, reason = process_single_file(name)
        if did_write:
            written.append(f"{OUTPUT_DIR_REL}{fname}")
        else:
            skipped.append({"file": fname, "reason": reason})

    return {"written": written, "count": len(written), "skipped": skipped}

def run_prompt(payload: dict) -> dict:
    """
    payload options:
      { "debug_headers": true, "header_preview": 40 }
    """
    logger.info("🚀 Starting Section/Sub-Section formatter")
    debug_headers = bool(payload.get("debug_headers", False))
    header_preview = int(payload.get("header_preview", 20))
    result = process_all_files(debug_headers=debug_headers, header_preview=header_preview)
    logger.info(f"🏁 Completed: {result}")
    return result

if __name__ == "__main__":
    print(json.dumps(run_prompt({}), indent=2))
