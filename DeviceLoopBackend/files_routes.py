# files_routes.py
from __future__ import annotations

from flask import Blueprint, request, jsonify
from .guards import require_role
from .s3_utils import presign_put, presign_get

bp = Blueprint("files", __name__)

@bp.post("/sign-put")
@require_role("sellers", "admin")
def sign_put():
    """
    Request body: { "key": "listings/SELLER#/uuid-front.jpg", "type": "image/jpeg" }
    Returns: { "url": "<presigned PUT URL>" }
    """
    data = request.get_json(force=True) or {}
    key = (data.get("key") or "").strip()
    content_type = (data.get("type") or "").strip()

    if not key or not content_type:
        return jsonify({"error": "key and type are required"}), 400

    url = presign_put(key, content_type)
    return jsonify({ "url": url })


@bp.get("/sign-get")
def sign_get():
    """
    Query string: ?key=listings/SELLER#/uuid-front.jpg
    Returns: { "url": "<presigned GET URL>" }
    """
    key = (request.args.get("key") or "").strip()
    if not key:
        return jsonify({"error": "key is required"}), 400

    url = presign_get(key)
    return jsonify({ "url": url })
