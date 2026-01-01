# DeviceLoopBackend/__init__.py
import os
import boto3
from dotenv import load_dotenv
from flask import Flask, request, jsonify, make_response
from flask_cors import CORS
from werkzeug.middleware.proxy_fix import ProxyFix
from authlib.integrations.flask_client import OAuth

load_dotenv()


def _allowed_origins() -> list[str]:
    defaults = [
        "https://deviceloop.online",
        "https://www.deviceloop.online",
    ]
    extra = (os.getenv("FRONTEND_ORIGINS") or "").strip()
    if extra:
        more = [x.strip() for x in extra.split(",") if x.strip()]
        out: list[str] = []
        for o in defaults + more:
            if o not in out:
                out.append(o)
        return out
    return defaults


def create_app():
    app = Flask(__name__)
    # Trust Nginx forwarded headers (scheme/host) so OAuth redirects use https://deviceloop.online
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)

    app.url_map.strict_slashes = False

    # Secret key
    app.secret_key = os.getenv( "FLASK_SECRET_KEY", "0481ee12a0cfbf8f9dff44073b5164adda000bd84b68657166b60eeba486a68b",
    )
    secret = os.getenv("FLASK_SECRET_KEY") or os.getenv("SECRET_KEY")
    if not secret:
        raise RuntimeError("Missing FLASK_SECRET_KEY (or SECRET_KEY) in environment")

    app.secret_key = secret
    app.config["SECRET_KEY"] = secret


    cookie_domain = os.getenv("COOKIE_DOMAIN")  # example: .deviceloop.online
    if cookie_domain:
        app.config["SESSION_COOKIE_DOMAIN"] = cookie_domain

    app.config["SESSION_COOKIE_SECURE"] = True
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "None"

    # Dynamo database
    aws_region = os.getenv("AWS_REGION", "ap-southeast-1")
    ddb_table_name = os.getenv("DDB_TABLE", "DeviceLoop")
    ddb = boto3.resource("dynamodb", region_name=aws_region)
    app.ddb_table = ddb.Table(ddb_table_name)  # type: ignore[attr-defined]

    # Configuration used by your other modules
    app.config.update(
        AWS_REGION=os.getenv("AWS_REGION", "ap-southeast-1"),
        COGNITO_USERPOOL_ID=os.getenv("COGNITO_USERPOOL_ID"),
        COGNITO_CLIENT_ID=os.getenv("COGNITO_CLIENT_ID"),
        COGNITO_HOSTED_DOMAIN=os.getenv("COGNITO_HOSTED_DOMAIN"),
        FRONTEND_AFTER_LOGIN=os.getenv("FRONTEND_AFTER_LOGIN"),
        FRONTEND_AFTER_LOGOUT=os.getenv("FRONTEND_AFTER_LOGOUT"),
        COGNITO_SIGNOUT_CALLBACK=os.getenv("COGNITO_SIGNOUT_CALLBACK"),
        AWS_LOCATION_INDEX=os.getenv("AWS_LOCATION_INDEX"),
        BIDS_QUEUE_URL=os.environ.get("BIDS_QUEUE_URL"),
        S3_UPLOADS_BUCKET=os.getenv("S3_UPLOADS_BUCKET"),
        S3_PRESIGN_EXPIRE=int(os.getenv("S3_PRESIGN_EXPIRE", "900")),
        SESSION_COOKIE_NAME="deviceloop_sess",
        SESSION_COOKIE_SAMESITE="None",
        SESSION_COOKIE_SECURE=True,
        PREFERRED_URL_SCHEME="https",
        SESSION_COOKIE_HTTPONLY=True,
    )

    allowed = _allowed_origins()

    # Global Cross Origin Resource Sharing (applies to EVERYTHING, including 404 responses)
    CORS(
        app,
        supports_credentials=True,
        origins=allowed,
        resources={r"/*": {"origins": allowed}},
    )

    # Global preflight handler (so OPTIONS never returns 404)
    @app.before_request
    def _handle_preflight():
        if request.method == "OPTIONS":
            return make_response("", 204)
        return None

    # Force headers onto any response (including errors)
    @app.after_request
    def _force_headers(resp):
        origin = request.headers.get("Origin")
        if origin and origin in allowed:
            resp.headers["Access-Control-Allow-Origin"] = origin
            resp.headers["Access-Control-Allow-Credentials"] = "true"
            resp.headers["Vary"] = "Origin"
            resp.headers["Access-Control-Allow-Headers"] = request.headers.get(
                "Access-Control-Request-Headers",
                "Content-Type, Authorization, X-Requested-With",
            )
            resp.headers["Access-Control-Allow-Methods"] = request.headers.get(
                "Access-Control-Request-Method",
                "GET, POST, PUT, PATCH, DELETE, OPTIONS",
            )
        return resp

    # Cognito OpenID Connect
    oauth = OAuth(app)
    cognito_domain = os.getenv("COGNITO_DOMAIN", "https://cognito-idp.ap-southeast-1.amazonaws.com")
    userpool_id = os.getenv("COGNITO_USERPOOL_ID")
    client_id = os.getenv("COGNITO_CLIENT_ID")
    client_secret = os.getenv("COGNITO_CLIENT_SECRET")
    issuer = f"{cognito_domain}/{userpool_id}"

    oauth.register(
        name="oidc",
        client_id=client_id,
        client_secret=client_secret,
        server_metadata_url=f"{issuer}/.well-known/openid-configuration",
        client_kwargs={"scope": "openid email phone profile"},
    )
    app.oauth = oauth  # type: ignore[attr-defined]

    # Import blueprints INSIDE create_app (avoids import-order issues)
    from .views import bp as views_bp
    from .auth_routes import bp as auth_bp
    from .admin_routes import bp as admin_bp
    from .devices_routes import bp as devices_bp
    from .seller_routes import bp as seller_bp
    from .files_routes import bp as files_bp
    from .buyer_routes import bp as buyer_bp

    app.register_blueprint(views_bp)                     # /api/...
    app.register_blueprint(auth_bp, url_prefix="/auth")  # /auth/...
    app.register_blueprint(admin_bp)
    app.register_blueprint(devices_bp)
    app.register_blueprint(seller_bp)
    app.register_blueprint(files_bp, url_prefix="/files")
    app.register_blueprint(buyer_bp)                     # /buyer/...

    # Debug endpoint to prove buyer routes exist
    @app.get("/debug/routes")
    def debug_routes():
        buyer_rules = sorted(
            [
                {"rule": r.rule, "methods": sorted(list(r.methods or []))}
                for r in app.url_map.iter_rules()
                if r.rule.startswith("/buyer")
            ],
            key=lambda x: x["rule"],
        )
        return jsonify({"buyerRoutes": buyer_rules})

    return app
