from flask import Flask, request, jsonify
import importlib
import threading
import os
from logger import logger
from Scripts.Predictive_Report.ingest_typeform import process_typeform_submission
from Scripts.Elasticity.elasticity_typeform import process_typeform_submission as process_elasticity_submission

app = Flask(__name__)

# --- ROUTE SAFEGUARD ---
RENDER_ENV = os.getenv("RENDER_ENV", "/ingest-typeform-test")  # Optional default for dev
if not RENDER_ENV.startswith("/"):
    raise RuntimeError(f"❌ Invalid or missing RENDER_ENV: {RENDER_ENV!r} — must start with '/'")

logger.info(f"📡 Flask binding RENDER_ENV route: {RENDER_ENV}")

# --- PROMPT ROUTING CONFIG ---
BLOCKING_PROMPTS = {
    "website",
    "year",
    "read_client_context",
    "read_question_context",
    "read_prompt_1_thinking",
    "write_change_effect_maths",
    "read_change_effect_maths",
    "read_prompt_2_section_assets",
    "read_prompt_3_report_assets",
    "read_prompt_4_tables",
    "combine",
    "format_combine",
    "read_section_image_prompts",
    "read_report_image_prompts",
    "format_image_prompts",
    "csv_content",
    "report_and_section_table_csv",
    "write_create_folders",
    "read_create_folders",
    "move_files_1",
    "move_files_2",
    "read_supply_report",
    "read_demand_report",
    "write_prompt_1_elasticity",
    "read_prompt_1_elasticity",
    "write_elasticity_maths",
    "elasticity_combine",
    "elasticity_csv",
    "write_create_elasticity_folders",
    "read_create_elasticity_folders"
    "move_elasticity_files_1",
    "move_elasticity_files_2"
}

PROMPT_MODULES = {
    "website": "Scripts.Website_Year.website",
    "year": "Scripts.Website_Year.year",
    "read_client_context": "Scripts.Client_Context.read_client_context",
    "write_client_context": "Scripts.Client_Context.write_client_context",
    "read_question_context": "Scripts.Predictive_Report.read_question_context",
    "write_prompt_1_thinking": "Scripts.Predictive_Report.write_prompt_1_thinking",
    "write_change_effect_maths": "Scripts.Predictive_Report.write_change_effect_maths",
    "read_change_effect_maths": "Scripts.Predictive_Report.read_change_effect_maths",
    "read_prompt_1_thinking": "Scripts.Predictive_Report.read_prompt_1_thinking",
    "write_prompt_2_section_assets": "Scripts.Predictive_Report.write_prompt_2_section_assets",
    "read_prompt_2_section_assets": "Scripts.Predictive_Report.read_prompt_2_section_assets",
    "write_prompt_3_report_assets": "Scripts.Predictive_Report.write_prompt_3_report_assets",
    "read_prompt_3_report_assets": "Scripts.Predictive_Report.read_prompt_3_report_assets",
    "write_prompt_4_tables": "Scripts.Predictive_Report.write_prompt_4_tables",
    "read_prompt_4_tables": "Scripts.Predictive_Report.read_prompt_4_tables",
    "combine": "Scripts.Predictive_Report.combine",
    "format_combine": "Scripts.Predictive_Report.format_combine",
    "write_section_image_prompts": "Scripts.Image_Prompts.write_section_image_prompts",
    "read_section_image_prompts": "Scripts.Image_Prompts.read_section_image_prompts",
    "write_report_image_prompts": "Scripts.Image_Prompts.write_report_image_prompts",
    "read_report_image_prompts": "Scripts.Image_Prompts.read_report_image_prompts",
    "format_image_prompts": "Scripts.Image_Prompts.format_image_prompts",
    "csv_content": "Scripts.Predictive_Report.csv_content",
    "report_and_section_table_csv": "Scripts.Predictive_Report.report_and_section_table_csv",
    "write_create_folders": "Scripts.Predictive_Report.write_create_folders",
    "read_create_folders": "Scripts.Predictive_Report.read_create_folders",
    "move_files_1": "Scripts.Predictive_Report.move_files_1",
    "move_files_2": "Scripts.Predictive_Report.move_files_2",
    "read_supply_report": "Scripts.Elasticity.read_supply_report",
    "read_demand_report": "Scripts.Elasticity.read_demand_report",
    "write_prompt_1_elasticity": "Scripts.Elasticity.write_prompt_1_elasticity",
    "read_prompt_1_elasticity": "Scripts.Elasticity.read_prompt_1_elasticity",
    "write_elasticity_maths": "Scripts.Elasticity.write_elasticity_maths",
    "elasticity_combine": "Scripts.Elasticity.elasticity_combine",
    "elasticity_csv": "Scripts.Elasticity.elasticity_csv",
    "write_create_elasticity_folders": "Scripts.Elasticity.write_create_elasticity_folders",
    "read_create_elasticity_folders": "Scripts.Elasticity.read_create_elasticity_folders",
    "move_elasticity_files_1": "Scripts.Elasticity.move_elasticity_files_1",
    "move_elasticity_files_2": "Scripts.Elasticity.move_elasticity_files_2"
}

# --- ROUTES ---
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

@app.route("/elasticity-typeform", methods=["POST"])
def elasticity_typeform_handler():
    try:
        data = request.get_json(force=True)
        logger.info("📩 Elasticity Typeform webhook received")
        process_elasticity_submission(data)
        return jsonify({"status": "success", "message": "Elasticity files processed and saved."})
    except Exception as e:
        logger.exception("❌ Error handling Elasticity Typeform submission")
        return jsonify({"status": "error", "message": str(e)}), 500

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
        logger.info(f"Dispatching prompt asynchronously: {prompt_name}")
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
        logger.exception("Error in dispatch_prompt")
        return jsonify({"error": str(e)}), 500
