from flask import Flask, request, jsonify
import importlib
import threading
import os
from logger import logger
from Scripts.JSON_to_csv.ingest_typeform import process_typeform_submission

app = Flask(__name__)

# --- ROUTE CONFIG ---
RENDER_ENV = os.getenv("RENDER_ENV", "/ingest-typeform")
if not RENDER_ENV.startswith("/"):
    raise RuntimeError(f"❌ Invalid or missing RENDER_ENV: {RENDER_ENV!r} — must start with '/'")

logger.info(f"📡 Flask binding RENDER_ENV route: {RENDER_ENV}")

# --- TYPEFORM INGESTION ROUTE ---
@app.route(RENDER_ENV, methods=["POST"])
def dynamic_ingest_typeform():
    try:
        data = request.get_json(force=True)
        logger.info(f"📩 Typeform webhook received via {RENDER_ENV}")
        process_typeform_submission(data)
        return jsonify({"status": "success", "message": "Files processed and saved to Supabase."})
    except Exception as e:
        logger.exception(f"❌ Error handling Typeform submission via {RENDER_ENV}")
        return jsonify({"status": "error", "message": str(e)}), 500

# --- BLOCKING VS ASYNC PROMPTS (can expand later) ---
BLOCKING_PROMPTS = {
    "JSON_to_csv",
    "delete_input_output_files",
}

PROMPT_MODULES = {
    "JSON_to_csv": "Scripts.JSON_to_csv.convert_json_to_csv",
    "delete_input_output_files": "Scripts.JSON_to_csv.delete_input_output_files",
}

# --- ZAPIER/JSON DISPATCH ROUTE ---
@app.route("/", methods=["POST"])
def dispatch_prompt():
    try:
        data = request.get_json(force=True)
        prompt_name = data.get("prompt")
        if not prompt_name:
            return jsonify({"error": "Missing 'prompt' key"}), 400

        module_path = PROMPT_MODULES.get(prompt_name)
        if not module_path:
            return jsonify({"error": f"Unknown prompt: {prompt_name}"}), 400

        module = importlib.import_module(module_path)
        logger.info(f"🚀 Dispatching prompt: {prompt_name}")
        result_container = {}

        import uuid
        if prompt_name not in BLOCKING_PROMPTS:
            run_id = data.get("run_id") or str(uuid.uuid4())
            data["run_id"] = run_id
            result_container["run_id"] = run_id

        def run_and_capture():
            try:
                result = module.run_prompt(data)
                result_container.update(result or {})
            except Exception:
                logger.exception("Background prompt execution failed.")

        thread = threading.Thread(target=run_and_capture)
        thread.start()

        if prompt_name in BLOCKING_PROMPTS:
            thread.join()
            return jsonify(result_container)

        return jsonify({
            "status": "processing",
            "message": "Script launched, run_id will be available via follow-up.",
            "run_id": result_container.get("run_id")
        })

    except Exception as e:
        logger.exception("❌ Error in dispatch_prompt")
        return jsonify({"error": str(e)}), 500
