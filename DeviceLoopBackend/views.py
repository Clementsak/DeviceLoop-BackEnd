from flask import Blueprint, jsonify, session, current_app
from .auth_routes import _find_user_pk_by_sub, _profile_key

bp = Blueprint("views", __name__)

@bp.get("/api/health")
def health():
    return jsonify(status="ok")

@bp.get("/api/me")
def me():
    u = session.get("user")
    if not u:
        return jsonify(user=None)

    table = current_app.ddb_table
    sub   = u["sub"]
    pk    = _find_user_pk_by_sub(table, sub)

    # ---- defaults from profile ----
    profile_role = "buyers"
    profile_groups = []
    verified = False

    if pk:
        r = table.get_item(Key=_profile_key(pk), ConsistentRead=True).get("Item", {}) or {}
        profile_role   = r.get("Role", "buyers")
        profile_groups = r.get("Groups") or []
        verified       = bool(r.get("IsVerified", False))

    # ---- groups from Cognito session (if present) ----
    cognito_groups = u.get("cognito:groups") or u.get("groups") or []
    if not isinstance(cognito_groups, list):
        cognito_groups = []

    # ---- merge + normalize ----
    all_groups = sorted({
        str(g).lower()
        for g in [*cognito_groups, *profile_groups]
        if isinstance(g, str) and g
    })

    # ---- derive effective role (admin > sellers > profile) ----
    effective_role = (
        "admin" if "admin" in all_groups else
        "sellers" if "sellers" in all_groups else
        profile_role
    )

    return jsonify(user={
        "sub": sub,
        "email": u.get("email"),
        "phone_number": u.get("phone_number"),
        "role": effective_role,
        "groups": all_groups,                       # ALWAYS an array
        "verified": verified or bool(u.get("email_verified", False)),
    })
