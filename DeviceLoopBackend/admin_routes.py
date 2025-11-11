# DeviceLoopBackend/admin_routes.py
import base64, json, os
from datetime import datetime, timezone
from flask import Blueprint, request, jsonify, current_app
from botocore.exceptions import ClientError
import boto3
from boto3.dynamodb.conditions import Key, Attr

from .guards import require_role
from .auth_routes import (
    _find_user_pk_by_sub,  # already have
    _cognito_username_from_sub,
    role_from_groups,
    _profile_key
)


def _gsi1() -> str: return current_app.config.get("DDB_GSI1", "GSI1")
def _gsi2() -> str: return current_app.config.get("DDB_GSI2", "GSI2")
def _gsi3() -> str: return current_app.config.get("DDB_GSI3", "GSI3")

def _idp():
    region = current_app.config.get("AWS_REGION", "ap-southeast-1")
    return boto3.client("cognito-idp", region_name=region)

def _user_pool_id() -> str:
    upid = current_app.config.get("COGNITO_USERPOOL_ID")
    if not upid:
        raise RuntimeError("COGNITO_USERPOOL_ID is not configured")
    return upid

bp = Blueprint("admin", __name__, url_prefix="/admin")

# --- Helpers ---
def _iso_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _encode_cursor(key: dict | None) -> str | None:
    if not key: return None
    return base64.urlsafe_b64encode(json.dumps(key).encode()).decode()

def _decode_cursor(s: str | None) -> dict | None:
    if not s: return None
    return json.loads(base64.urlsafe_b64decode(s.encode()).decode())

def _is_email(s: str) -> bool:
    return "@" in s and "." in s

def _get_profile(table, user_pk: str) -> dict | None:
    r = table.get_item(Key=_profile_key(user_pk), ConsistentRead=True)
    return r.get("Item")

def _list_by_role(table, role: str, limit: int, cursor: str | None):
    kwargs = {
        "IndexName": _gsi1(),
        "KeyConditionExpression": Key("GSI1PK").eq(f"ROLE#{role}"),
        "FilterExpression": Attr("Status").ne("deleted"),
        "Limit": limit,
    }
    lek = _decode_cursor(cursor)
    if lek: kwargs["ExclusiveStartKey"] = lek
    resp = table.query(**kwargs)
    return resp.get("Items", []), _encode_cursor(resp.get("LastEvaluatedKey"))

def _list_verify_queue(table, kind: str, limit: int, cursor: str | None):
    kwargs = {
        "IndexName": _gsi2(),
        "KeyConditionExpression": Key("GSI2PK").eq(f"VERIFY#PENDING#{kind}"),
        "Limit": limit,
        "ScanIndexForward": True,  # oldest first
    }
    lek = _decode_cursor(cursor)
    if lek:
        kwargs["ExclusiveStartKey"] = lek

    resp = table.query(**kwargs)
    return resp.get("Items", []), _encode_cursor(resp.get("LastEvaluatedKey"))

def _sync_role_ddb(table, user_pk: str, new_role: str, groups: list[str]):
    table.update_item(
        Key=_profile_key(user_pk),
        UpdateExpression="SET #role=:r, Groups=:g, GSI1PK=:g1pk, GSI1SK=:g1sk",
        ExpressionAttributeNames={"#role": "Role"},
        ExpressionAttributeValues={
            ":r": new_role,
            ":g": groups,
            ":g1pk": f"ROLE#{new_role}",
            ":g1sk": user_pk,
        },
    )

# --- Routes ---

@bp.get("/users")
@require_role("admin")
def list_users():
    table = current_app.ddb_table
    q        = (request.args.get("q") or "").strip()
    role     = (request.args.get("role") or "").strip()
    verified = (request.args.get("verified") or "").strip()
    limit    = max(1, min(int(request.args.get("limit", "25")), 100))
    cursor   = request.args.get("cursor")

    items: list[dict]
    next_cursor: str | None = None

    if q.startswith("USER#"):
        prof = _get_profile(table, q)
        items = [prof] if prof else []

    elif _is_email(q):
        resp = table.query(
            IndexName=_gsi3(),
            KeyConditionExpression=Key("GSI3PK").eq(f"EMAIL#{q.lower()}"),
            Limit=1,
        )
        items = []
        for it in resp.get("Items", []):
            pk = it["GSI3SK"]
            prof = _get_profile(table, pk)
            if prof and prof.get("Status") != "deleted":
                items.append(prof)

    elif role in ("buyers", "sellers", "admin"):
        items, next_cursor = _list_by_role(table, role, limit, cursor)

    else:
        # Default: list ALL users (profile items), but ignore soft-deleted.
        fe = (
            Attr("SK").eq("PROFILE")
            & Attr("Type").eq("User")
            & Attr("Status").ne("deleted")
        )
        scan_kwargs = {"FilterExpression": fe, "Limit": limit}
        lek = _decode_cursor(cursor)
        if lek: scan_kwargs["ExclusiveStartKey"] = lek
        resp = table.scan(**scan_kwargs)
        items = resp.get("Items", [])
        next_cursor = _encode_cursor(resp.get("LastEvaluatedKey"))

    if verified in {"pending", "verified", "rejected"}:
        items = [i for i in items if i.get("VerifiedStatus", "pending") == verified]

    users = [{
        "user_pk": it["PK"],
        "email": it.get("Email"),
        "role": it.get("Role", "buyers"),
        "isVerified": bool(it.get("IsVerified", False)),
        "verifiedStatus": it.get("VerifiedStatus", "pending"),
        "lastLogin": it.get("LastLoginAt"),
        "groups": it.get("Groups", []),
    } for it in items if it]

    return jsonify(items=users, cursor=next_cursor)



@bp.patch("/users/<user_pk>/role")
@require_role("admin")
def change_role(user_pk: str):
    """
    Body: {"newRole": "buyers" | "sellers"}
    Admin cannot be set here (manual only).
    """
    body = request.get_json(silent=True) or {}
    new_role = body.get("newRole")
    if new_role not in ("buyers", "sellers"):
        return jsonify(error="Only buyers/sellers allowed here."), 400

    table = current_app.ddb_table
    prof = _get_profile(table, user_pk)
    if not prof:
        return jsonify(error="User not found"), 404

    # Get sub -> username
    sub = prof.get("Sub")
    if not sub:
        return jsonify(error="Missing Sub on profile"), 400
    username = None
    try:
        r = _idp().list_users(UserPoolId=_user_pool_id(), Filter=f'sub = "{sub}"', Limit=1)
        u = r.get("Users", [])
        username = u[0]["Username"] if u else None
    except ClientError:
        pass
    if not username:
        return jsonify(error="Cognito user not found"), 404

    # Remove old group (if buyers/sellers) and add the new one
    old_role = prof.get("Role", "buyers")
    for g in (old_role, ):
        if g in ("buyers", "sellers"):
            try:
                _idp().admin_remove_user_from_group(UserPoolId=_user_pool_id(), Username=username, GroupName=g)
            except ClientError:
                pass
    try:
        _idp().admin_add_user_to_group(UserPoolId=_user_pool_id(), Username=username, GroupName=new_role)
    except ClientError as e:
        return jsonify(error=f"Failed to add to group: {e.response['Error']['Message']}"), 400

    # Refresh groups from Cognito and persist to DDB
    gr = _idp().admin_list_groups_for_user(UserPoolId=_user_pool_id(), Username=username).get("Groups", [])
    groups = [g["GroupName"] for g in gr]
    _sync_role_ddb(table, user_pk, new_role, groups)

    return jsonify(ok=True, user_pk=user_pk, role=new_role, groups=groups)


@bp.delete("/users/<user_pk>")
@require_role("admin")
def delete_user(user_pk: str):
    hard = request.args.get("hard") == "true"
    table = current_app.ddb_table
    if hard:
        table.delete_item(Key=_profile_key(user_pk))
        # optionally also delete SUB# map if you keep it: {"PK": f"SUB#{sub}", "SK":"MAP"}
    else:
        table.update_item(
            Key=_profile_key(user_pk),
            UpdateExpression="SET #s=:d REMOVE GSI1PK, GSI1SK, GSI3PK, GSI3SK",
            ExpressionAttributeNames={"#s": "Status"},
            ExpressionAttributeValues={":d": "deleted"},
        )
    return jsonify(ok=True, user_pk=user_pk, hard=hard)


@bp.get("/verify/queue")
@require_role("admin")
def verify_queue():
    """
    ?type=user|seller, ?limit, ?cursor
    """
    kind = request.args.get("type", "user")
    if kind not in ("user", "seller"):
        return jsonify(error="type must be user|seller"), 400

    limit = max(1, min(int(request.args.get("limit", "25")), 100))
    cursor = request.args.get("cursor")
    table = current_app.ddb_table

    items, next_cursor = _list_verify_queue(table, kind, limit, cursor)
    # Normalize output
    out = []
    for it in items:
        out.append({
            "user_pk": it["PK"],
            "submittedAt": it.get("SubmittedAt"),
            "type": "VerifySeller" if "SELLER" in it["SK"] else "VerifyUser",
            "data": it.get("Data"),
            "sellerProfile": it.get("SellerProfile"),
            # Future: include geolocation fields if present in Data
        })
    return jsonify(items=out, cursor=next_cursor)


@bp.post("/verify/<user_pk>/decision")
@require_role("admin")
def verify_decision(user_pk: str):
    """
    Body: { "type": "user"|"seller", "decision": "approve"|"reject", "reason"?: string }
    """
    body = request.get_json(silent=True) or {}
    kind = body.get("type")
    decision = body.get("decision")
    reason = body.get("reason", "")

    if kind not in ("user", "seller"):
        return jsonify(error="type must be user|seller"), 400
    if decision not in ("approve", "reject"):
        return jsonify(error="decision must be approve|reject"), 400

    table = current_app.ddb_table
    sk = "VERIFY#USER#ACTIVE" if kind == "user" else "VERIFY#SELLER#ACTIVE"

    # 1) Load verify item
    r = table.get_item(Key={"PK": user_pk, "SK": sk}, ConsistentRead=True)
    v = r.get("Item")
    if not v:
        return jsonify(error="Active verification not found"), 404

    # 2) Update profile
    if decision == "approve":
        table.update_item(
            Key=_profile_key(user_pk),
            UpdateExpression="SET IsVerified=:t, VerifiedStatus=:vs, VerifiedAt=:ts",
            ExpressionAttributeValues={":t": True, ":vs": "verified", ":ts": _iso_now()},
        )
    else:
        table.update_item(
            Key=_profile_key(user_pk),
            UpdateExpression="SET IsVerified=:f, VerifiedStatus=:vs",
            ExpressionAttributeValues={":f": False, ":vs": "rejected"},
        )

    # 3) Remove from queue (drop GSI2 attributes) & stamp outcome
    # If you want to keep history, you can also duplicate it to VERIFY#...#HIST#<ts>
    expr_names = {}
    expr_vals = {":st": ("approved" if decision=="approve" else "rejected"), ":ts": _iso_now(), ":rs": reason}
    update = "SET #st=:st, DecidedAt=:ts, Reason=:rs REMOVE GSI2PK, GSI2SK"

    expr_names["#st"] = "Status"

    table.update_item(
        Key={"PK": user_pk, "SK": sk},
        UpdateExpression=update,
        ExpressionAttributeNames=expr_names,
        ExpressionAttributeValues=expr_vals,
    )

    # 4) TODO (later): enqueue notifications to SQS/SES

    return jsonify(ok=True, user_pk=user_pk, type=kind, decision=decision)
