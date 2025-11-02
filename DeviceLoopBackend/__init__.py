# __init__.py
import os
from flask import Flask
from flask_cors import CORS
from authlib.integrations.flask_client import OAuth
from dotenv import load_dotenv

load_dotenv()

def create_app():
    app = Flask(__name__)

    # Stable dev secret in .env
    app.secret_key = os.getenv("FLASK_SECRET_KEY", "0481ee12a0cfbf8f9dff44073b5164adda000bd84b68657166b60eeba486a68b")

    # __init__.py (your app factory)
    app.config.update(
    SESSION_COOKIE_NAME="deviceloop_sess",
    SESSION_COOKIE_SAMESITE="None",   # cross-site during Hosted UI redirects
    SESSION_COOKIE_SECURE=True,       # requires HTTPS
    PREFERRED_URL_SCHEME="https",
    )

# CORS: your frontend is now https://localhost:5173
    CORS(
    app,
    resources={r"/api/*": {"origins": ["https://localhost:5173"]}},
    supports_credentials=True,
    )


    # --- Authlib (Cognito OIDC) ---
    oauth = OAuth(app)
    COGNITO_DOMAIN = os.getenv("COGNITO_DOMAIN", "https://cognito-idp.ap-southeast-1.amazonaws.com")
    USERPOOL_ID    = os.getenv("COGNITO_USERPOOL_ID", "ap-southeast-1_aS6juaTBr")
    CLIENT_ID      = os.getenv("COGNITO_CLIENT_ID", "1erabgmlso22dt99armn0d0un1")
    CLIENT_SECRET  = os.getenv("COGNITO_CLIENT_SECRET", "<client secret>")
    ISSUER         = f"{COGNITO_DOMAIN}/{USERPOOL_ID}"

    oauth.register(
        name="oidc",
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        server_metadata_url=f"{ISSUER}/.well-known/openid-configuration",
        client_kwargs={"scope": "openid email phone"},
    )
    app.oauth = oauth  # type: ignore[attr-defined]

    from .views import bp as views_bp
    from .auth_routes import bp as auth_bp
    app.register_blueprint(views_bp)                    # /api/...
    app.register_blueprint(auth_bp, url_prefix="/auth") # /auth/...

    return app
