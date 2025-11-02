from flask import Blueprint, jsonify, session

bp = Blueprint("views", __name__)

@bp.get("/api/health")
def health():
    return jsonify(status="ok")

@bp.get("/api/me")
def me():
    # front end can poll this to know if a user is logged in
    return jsonify(user=session.get("user"))
