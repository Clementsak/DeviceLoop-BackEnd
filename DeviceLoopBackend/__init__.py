# __init__.py
import os
from flask import Flask
from flask_cors import CORS
from authlib.integrations.flask_client import OAuth
from dotenv import load_dotenv
import boto3

load_dotenv()

from .admin_routes import bp as admin_bp
from .devices_routes import bp as devices_bp
from .seller_routes import bp as seller_bp
from .files_routes import bp as files_bp
from .buyer_routes import bp as buyer_bp

load_dotenv()

def create_app():
    app = Flask(__name__)

    # Stable dev secret in .env
    app.secret_key = os.getenv("FLASK_SECRET_KEY", "0481ee12a0cfbf8f9dff44073b5164adda000bd84b68657166b60eeba486a68b")

    # --- DynamoDB (one-time setup) ---
    aws_region = os.getenv("AWS_REGION", "ap-southeast-1")
    ddb_table_name = os.getenv("DDB_TABLE", "DeviceLoop")
    ddb = boto3.resource("dynamodb", region_name=aws_region)
    app.ddb_table = ddb.Table(ddb_table_name)  # type: ignore[attr-defined]

    #dynamodb
    DDB_GSI1=os.getenv("DDB_GSI1", "GSI1"),                # role listing
    DDB_GSI2=os.getenv("DDB_GSI2", "GSI2"),                # verify queue
    DDB_GSI3=os.getenv("DDB_GSI3", "GSI3"),                # email lookup
    DDB_GSI4=os.getenv("DDB_GSI4", "GSI4"),

    # __init__.py (your app factory)
    app.config.update(
    AWS_REGION=os.getenv("AWS_REGION", "ap-southeast-1"),
    COGNITO_USERPOOL_ID=os.getenv("COGNITO_USERPOOL_ID"),          # <- required
    COGNITO_CLIENT_ID=os.getenv("COGNITO_CLIENT_ID"),
    COGNITO_HOSTED_DOMAIN=os.getenv("COGNITO_HOSTED_DOMAIN"),
    FRONTEND_AFTER_LOGIN=os.getenv("FRONTEND_AFTER_LOGIN", "https://localhost:5173/"),
    FRONTEND_AFTER_LOGOUT=os.getenv("FRONTEND_AFTER_LOGOUT", "https://localhost:5173/"),
    COGNITO_SIGNOUT_CALLBACK=os.getenv("COGNITO_SIGNOUT_CALLBACK", "https://localhost:5000/auth/signout-callback"),
    AWS_LOCATION_INDEX=os.getenv("AWS_LOCATION_INDEX", "deviceloop-place-index"),
    BIDS_QUEUE_URL=os.environ.get("BIDS_QUEUE_URL"),


    S3_UPLOADS_BUCKET=os.getenv("S3_UPLOADS_BUCKET"),
    S3_PRESIGN_EXPIRE=int(os.getenv("S3_PRESIGN_EXPIRE", "900")),

    SESSION_COOKIE_NAME="deviceloop_sess",
    SESSION_COOKIE_SAMESITE="None",   # cross-site during Hosted UI redirects
    SESSION_COOKIE_SECURE=True,       # requires HTTPS
    PREFERRED_URL_SCHEME="https",
    )

# CORS: your frontend is now https://localhost:5173
    CORS(
        app,
        supports_credentials=True,
        resources={
            r"/auth/*":   {"origins": ["https://localhost:5173"]},
            r"/api/*":    {"origins": ["https://localhost:5173"]},
            r"/admin/*":  {"origins": ["https://localhost:5173"]},
            r"/seller/*": {"origins": ["https://localhost:5173"]},
            r"/buyer/*":  {"origins": ["https://localhost:5173"]},  # 👈 ADD THIS
            r"/files/*":  {"origins": ["https://localhost:5173"]},
        },
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
    app.register_blueprint(admin_bp)
    app.register_blueprint(devices_bp)
    app.register_blueprint(seller_bp)
    app.register_blueprint(files_bp, url_prefix="/files")
    app.register_blueprint(buyer_bp)

    return app
