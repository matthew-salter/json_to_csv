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

def list_folder(abs_prefix: str):
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
    # Lower, trim, normalize separators
    x = (s or "").strip().lower()
    x = x.replace(" ", "_").replace("-", "_")
    x = re.sub(r"_+", "_", x)
    return x

# Suffix patterns:
#  - Decimal: base_<N>.<M> or base_<N>_<M>
DEC_RE = re.compile(r"^(?P<base>.+?)[_](?P<maj>\d+)(?:[._-](?P<min>\d+))$")
#  - Integer: base_<N>   (ensure we didn't already match decimal)
INT_RE = re.compile(r"^(?P<base>.+?)[_](?P<num>\d+)$")

# -----------------------------------------------------------
# Core Transform — suffix-driven only
# -----------------------------------------------------------

def transform_by_suffix(df: pd.DataFrame) -> pd.DataFrame:
    if df.shape[0] == 0:
        return pd.DataFrame()

    row0 = df.iloc[0].to_dict()
    # Canonicalize headers
    items = [(_canon(k), v) for k, v in row0.items()]

    globals_map: Dict[str, Any] = {}          # no suffix
    sections: Dict[int, Dict[str, Any]] = {}  # int suffix
    subs: Dict[int, Dict[str, Dict[str, Any]]] = {}  # N -> { "N.M": {base: val} }

    # Track first-seen order for columns
    order_global: List[str] = []
    order_sec: List[str] = []
    order_sub: List[str] = []

    def _remember(order_list: List[str], key: str):
        if key not in order_list:
            order_list.append(key)

    for k, v in items:
        m_dec = DEC_RE.match(k)
        if m_dec:
            base = m_dec.group("base")
            maj = int(m_dec.group("maj"))
            min_ = int(m_dec.group("min"))
            snum = f"{maj}.{min_}"
            subs.setdefault(maj, {}).setdefault(snum, {})[base] = v
            _remember(order_sub, base)
            continue

        m_int = INT_RE.match(k)
        if m_int:
            base = m_int.group("base")
            num = int(m_int.group("num"))
            sections.setdefault(num, {})[base] = v
            _remember(order_sec, base)
            continue

        # No suffix → global
        globals_map[k] = v
        _remember(order_global, k)

    # Collision handling: base name appears in both section & sub buckets
    sec_bases = set(order_sec)
    sub_bases = set(order_sub)
    overlap = sec_bases & sub_bases

    sec_out_cols = []
    sub_out_cols = []
    # Map output column -> source base
    sec_out_map: Dict[str, str] = {}
    sub_out_map: Dict[str, str] = {}

    for b in order_sec:
        col = f"section_{b}" if b in overlap else b
        sec_out_cols.append(col)
        sec_out_map[col] = b

    for b in order_sub:
        col = f"sub_section_{b}" if b in overlap else b
        sub_out_cols.append(col)
        sub_out_map[col] = b

    # Assemble final column order
    columns = []
    columns.extend(order_global)            # globals, e.g. report_change, report_title, etc.
    columns.append("section_number")
    columns.extend(sec_out_cols)
    columns.append("sub_section_number")
    columns.extend(sub_out_cols)

    # Build rows
    out_rows: List[Dict[str, Any]] = []
    section_nums = sorted(sections.keys())
    logger.info(f"🔎 Suffix-based detection → sections: {section_nums or 'NONE'} | has_subs_for: {sorted(list(subs.keys())) or 'NONE'}")

    if not section_nums and subs:
        # There are sub-sections but no explicit section_*_N keys: infer sections from subs' majors
        section_nums = sorted(subs.keys())

    if not section_nums and not subs:
        # Nothing to materialize
        logger.warning("⚠️ No section or sub-section indices found by suffix. Returning empty frame.")
        return pd.DataFrame(columns=columns)

    for sec_num in section_nums:
        sec_fields = sections.get(sec_num, {})
        sub_map = subs.get(sec_num, {})

        if sub_map:
            for sub_num_str in sorted(sub_map.keys(), key=lambda s: (int(s.split(".")[0]), int(s.split(".")[1]))):
                sub_fields = sub_map[sub_num_str]
                rec: Dict[str, Any] = {}

                # Globals repeated
                for g in order_global:
                    rec[g] = globals_map.get(g)

                rec["section_number"] = int(sec_num)

                # Section fields
                for col in sec_out_cols:
                    base = sec_out_map[col]
                    rec[col] = sec_fields.get(base)

                # Sub fields
                rec["sub_section_number"] = float(sub_num_str)
                for col in sub_out_cols:
                    base = sub_out_map[col]
                    rec[col] = sub_fields.get(base)

                out_rows.append(rec)
        else:
            # Section without sub-rows => single row with blank sub fields
            rec = {}
            for g in order_global:
                rec[g] = globals_map.get(g)
            rec["section_number"] = int(sec_num)
            for col in sec_out_cols:
                base = sec_out_map[col]
                rec[col] = sec_fields.get(base)
            rec["sub_section_number"] = pd.NA
            for col in sub_out_cols:
                rec[col] = pd.NA
            out_rows.append(rec)

    out_df = pd.DataFrame(out_rows, columns=columns)
    logger.info(f"🧮 Built rows: {len(out_df)} | columns: {len(columns)}")
    return out_df

# -----------------------------------------------------------
# Orchestration
# -----------------------------------------------------------

def process_single_file(filename: str) -> Tuple[str, bool, str]:
    in_rel = f"{INPUT_DIR_REL}{filename}"
    out_rel = f"{OUTPUT_DIR_REL}{filename}"

    logger.info(f"📥 Reading XLSX (rel): {in_rel}")
    df_in = read_xlsx_from_supabase(in_rel)

    logger.info("🔧 Transforming via suffix-only logic...")
    df_out = transform_by_suffix(df_in)

    if df_out.shape[0] == 0:
        logger.warning(f"⏭️ Skipping write for '{filename}': no rows built.")
        return (filename, False, "no rows")

    logger.info(f"📤 Writing XLSX (rel): {out_rel}")
    write_xlsx_to_supabase(df_out, out_rel)

    logger.info(f"✅ Done: {out_rel}")
    return (filename, True, "")

def process_all_files() -> Dict[str, Any]:
    entries = list_folder(LIST_DIR_ABS)
    written, skipped = [], []

    for e in entries:
        name = e.get("name")
        if not name or name.endswith("/"):
            continue
        if not name.lower().endswith((".xlsx", ".xls")):
            logger.info(f"⏭️ Skipping non-Excel file: {name}")
            continue

        try:
            fname, did_write, reason = process_single_file(name)
            if did_write:
                written.append(f"{OUTPUT_DIR_REL}{fname}")
            else:
                skipped.append({"file": fname, "reason": reason})
        except Exception as ex:
            logger.error(f"❌ Failed to process '{name}': {ex}")
            skipped.append({"file": name, "reason": str(ex)})

    return {"written": written, "count": len(written), "skipped": skipped}

def run_prompt(_: dict) -> dict:
    logger.info("🚀 Starting suffix-based formatter")
    result = process_all_files()
    logger.info(f"🏁 Completed: {result}")
    return result

if __name__ == "__main__":
    print(json.dumps(run_prompt({}), indent=2))
