import os
import logging
from flask import Flask, jsonify
from flask_cors import CORS
from config.settings import config_map
from app.extensions import init_extensions

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_app(config_name="production"):
    app = Flask(__name__)
    
    # Load configuration
    config_obj = config_map.get(config_name, config_map["production"])
    app.config.from_object(config_obj)
    
    # Initialize extensions
    # STEP 5 — ENABLE CORS (IMPORTANT)
    CORS(app, resources={r"/*": {"origins": "*"}})
    init_extensions(app)
    
    # Minimal health check (inline as backup)
    @app.route("/")
    def home():
        return "Server is running bro 🚀"

    @app.route("/health")
    def health_check():
        return jsonify({"status": "ok", "message": "CareVerify API is running", "version": "1.0.1-fixed"}), 200

    # STEP 3 — TEST API DIRECTLY
    @app.route("/test-api")
    def test_api():
        return jsonify({"status": "API WORKING"}), 200

    # Register Blueprints safely
    register_blueprints(app)
    
    return app

def register_blueprints(app):
    """Register all blueprints with error handling to ensure server starts."""
    blueprints = [
        ("app.api.health", "health_bp"),
        ("app.api.auth", "auth_bp"),
        ("app.api.claims", "claims_bp"),
        ("app.api.analytics", "analytics_bp"),
        ("app.api.admin", "admin_bp"),
        ("app.api.documents", "documents_bp"),
        ("app.api.notifications", "notifications_bp"),
        ("app.api.organizations", "org_bp"),
    ]
    
    for module_path, bp_name in blueprints:
        try:
            import importlib
            module = importlib.import_module(module_path)
            blueprint = getattr(module, bp_name)
            app.register_blueprint(blueprint, url_prefix="/api")
            print(f"[SUCCESS] Registered blueprint: {bp_name} from {module_path}")
        except Exception as e:
            print(f"[ERROR] Failed to register blueprint {bp_name} from {module_path}: {e}")
            import traceback
            traceback.print_exc()