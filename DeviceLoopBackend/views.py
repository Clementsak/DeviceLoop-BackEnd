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

    role     = "buyers"
    groups   = []
    verified = False

    if pk:
        r = table.get_item(Key=_profile_key(pk), ConsistentRead=True).get("Item", {})
        role     = r.get("Role", "buyers")
        groups   = r.get("Groups", [])
        verified = bool(r.get("IsVerified", False))

    return jsonify(user={
        "sub": u["sub"],
        "email": u.get("email"),
        "phone_number": u.get("phone_number"),
        "role": role,
        "groups": groups,
        "verified": verified,
    })