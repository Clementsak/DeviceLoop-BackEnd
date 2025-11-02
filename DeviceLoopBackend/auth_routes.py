# auth_routes.py
import os
from urllib.parse import quote
from flask import Blueprint, current_app, redirect, session, url_for

bp = Blueprint("auth", __name__)

FRONTEND_AFTER_LOGIN  = os.getenv("FRONTEND_AFTER_LOGIN",  "http://localhost:5173/")
FRONTEND_AFTER_LOGOUT = os.getenv("FRONTEND_AFTER_LOGOUT", "http://localhost:5173/")
HOSTED = os.getenv("COGNITO_HOSTED_DOMAIN")  # e.g. ap-southeast-1-xxxx.auth.ap-southeast-1.amazoncognito.com
CLIENT_ID = os.getenv("COGNITO_CLIENT_ID")
SIGNOUT_CALLBACK       = os.getenv("COGNITO_SIGNOUT_CALLBACK", "https://localhost:5000/auth/signout-callback")

@bp.get("/login")
def login():
# auth_routes.py
    redirect_uri = url_for("auth.callback", _external=True, _scheme="https")
    # prompt=login forces the account chooser every time (good for switching accounts)
    return current_app.oauth.oidc.authorize_redirect(redirect_uri, prompt="login")

@bp.get("/signup")
def signup():
    # auth_routes.py
    redirect_uri = url_for("auth.callback", _external=True, _scheme="https")
    # screen_hint=signup opens the Hosted UI on the sign-up screen, still with proper state
    return current_app.oauth.oidc.authorize_redirect(redirect_uri, screen_hint="signup")

@bp.get("/callback")
def callback():
    token = current_app.oauth.oidc.authorize_access_token()
    user  = token.get("userinfo") or {}
    session["user"] = {
        "sub": user.get("sub"),
        "email": user.get("email"),
        "phone_number": user.get("phone_number"),
    }
    return redirect(FRONTEND_AFTER_LOGIN)

@bp.get("/logout")
@bp.post("/logout")
def logout():
    """Clear Flask session and redirect through Cognito logout flow."""
    session.clear()

    # Step 1: Hosted UI logout
    logout_url = (
        f"https://{HOSTED}/logout"
        f"?client_id={CLIENT_ID}"
        f"&logout_uri={quote(SIGNOUT_CALLBACK, safe='')}"
    )
    return redirect(logout_url)

@bp.get("/signout-callback")
def signout_callback():
    """Final hop after Cognito logout — back to frontend."""
    return redirect(FRONTEND_AFTER_LOGOUT)


