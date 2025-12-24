# DeviceLoopBackend/admin_routes.py
import base64, json, os
from datetime import datetime, timezone
from flask import Blueprint, request, jsonify, current_app, session
from botocore.exceptions import ClientError
import boto3
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal

from .guards import require_role
from .auth_routes import (
    _find_user_pk_by_sub,  # already have
    _cognito_username_from_sub,
    role_from_groups,
    _profile_key,
    _sub_map_key,
)

def _table():
    return current_app.ddb_table

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
        "FilterExpression": Attr("Status").ne("deleted") & Attr("PK").begins_with("USER#"),
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

def _cognito_ver_flags(sub: str | None) -> dict:
    """
    Returns {'emailVerified': bool|None, 'phoneVerified': bool|None}
    """
    if not sub:
        return {"emailVerified": None, "phoneVerified": None}
    try:
        resp = _idp().admin_get_user(
            UserPoolId=_user_pool_id(),
            Username=sub,
        )
    except ClientError:
        return {"emailVerified": None, "phoneVerified": None}

    attrs = {a["Name"]: a["Value"] for a in resp.get("UserAttributes", [])}
    return {
        "emailVerified": attrs.get("email_verified") == "true",
        "phoneVerified": attrs.get("phone_number_verified") == "true",
    }


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
        scan_kwargs = {
            "FilterExpression": Attr("SK").eq("PROFILE") & Attr("Status").ne("deleted"),
            "Limit": limit,
        }

        if cursor:
            scan_kwargs["ExclusiveStartKey"] = cursor

        resp = table.scan(**scan_kwargs)
        items = resp.get("Items", [])
        next_cursor = _encode_cursor(resp.get("LastEvaluatedKey"))

    if verified in {"pending", "verified", "rejected"}:
        items = [i for i in items if i.get("VerifiedStatus", "pending") == verified]

    users: list[dict] = []
    for it in items:
        if not it:
            continue

        sub = it.get("Sub")
        flags = _cognito_ver_flags(sub)

        pk = it.get("PK", "")
        if not pk.startswith("USER#"):
            continue

        users.append({
            "user_pk": it["PK"],
            "email": it.get("Email"),
            "role": it.get("Role", "buyers"),

            # Your DDB-level buyer verification (location + admin decision)
            "isVerified": bool(it.get("IsVerified", False)),
            "verifiedStatus": it.get("VerifiedStatus", "pending"),

            # Cognito-level verification for login contact details
            "emailVerified": flags["emailVerified"],
            "phoneVerified": flags["phoneVerified"],

            "lastLogin": it.get("LastLoginAt"),
            "groups": it.get("Groups", []),
        })


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


@bp.route("/users/<path:user_pk>", methods=["DELETE"])
@require_role("admin")
def delete_user(user_pk):
    """
    Default behaviour is now HARD delete:
    1) Delete user in Amazon Cognito (frees email in the user pool)
    2) Delete user profile + sub map in Amazon DynamoDB (removes from admin lists and role lookups)

    If you ever need the old behaviour, call:
      DELETE /admin/users/<userPk>?hard=false
    """
    # default hard delete unless explicitly disabled
    hard_arg = request.args.get("hard")
    hard_delete = True if hard_arg is None else (hard_arg.lower() == "true")

    table = current_app.ddb_table
    profile_key = _profile_key(user_pk)

    profile = table.get_item(Key=profile_key).get("Item")

    if hard_delete:
        # --- 1) Delete from Amazon Cognito ---
        # Try to discover the Amazon Cognito Username reliably (sub -> Username, fallback email)
        sub_value = None
        email_value = None
        if profile:
            sub_value = profile.get("Sub") or profile.get("sub")
            email_value = profile.get("Email") or profile.get("email")

        cognito_username = None
        if sub_value:
            cognito_username = _cognito_username_from_sub(sub_value)
        if not cognito_username and email_value:
            # fallback: search by email in Amazon Cognito
            try:
                idp = _idp()
                pool_id = _user_pool_id()

                resp = idp.list_users(
                    UserPoolId=pool_id,
                    Filter=f'sub = "{sub}"',
                    Limit=1
                )
                users = resp.get("Users", [])
                if users:
                    sub = users[0]["Username"]
            except Exception:
                cognito_username = cognito_username  # keep whatever we have

        # last resort: some pools use email directly as Username
        if not cognito_username and email_value:
            cognito_username = email_value

        if cognito_username:
            try:
                _idp.admin_delete_user(
                    UserPoolId=_user_pool_id,
                    Username=cognito_username,
                )
            except _idp.exceptions.UserNotFoundException:
                pass  # already gone from Amazon Cognito
            except Exception as e:
                # Do not block Amazon DynamoDB cleanup if Amazon Cognito deletion fails
                print(f"[admin_delete_user] failed for {user_pk}: {e}")

        # --- 2) Delete from Amazon DynamoDB ---
        # delete sub map if present (used by _find_user_pk_by_sub)
        if sub_value:
            try:
                table.delete_item(Key=_sub_map_key(sub_value))
            except Exception as e:
                print(f"[delete_sub_map] failed for {user_pk}: {e}")

        # delete the profile row (removes from admin lists)
        if profile:
            table.delete_item(Key=profile_key)

        return jsonify({"ok": True, "hard": True})

    # ---- Soft delete (kept for completeness) ----
    if not profile:
        return jsonify({"ok": True, "hard": False, "note": "profile not found"})

    expr = "SET #S = :deleted, DeletedAt = :ts REMOVE GSI1PK, GSI1SK, GSI3PK, GSI3SK"
    table.update_item(
        Key=profile_key,
        UpdateExpression=expr,
        ExpressionAttributeNames={"#S": "Status"},
        ExpressionAttributeValues={
            ":deleted": "deleted",
            ":ts": _iso_now(),
        },
    )
    return jsonify({"ok": True, "hard": False})



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

    if kind == "seller" and decision == "approve":
        prof = table.get_item(Key=_profile_key(user_pk), ConsistentRead=True).get("Item") or {}
        sub = prof.get("Sub")
        if not sub:
            return jsonify(error="Missing Sub on profile"), 400

        username = _cognito_username_from_sub(sub)
        if not username:
            return jsonify(error="Amazon Cognito user not found"), 404

        # Remove buyers group (safe if not present) and add sellers group
        try:
            _idp().admin_remove_user_from_group(
                UserPoolId=_user_pool_id(),
                Username=username,
                GroupName="buyers",
            )
        except ClientError:
            pass

        try:
            _idp().admin_add_user_to_group(
                UserPoolId=_user_pool_id(),
                Username=username,
                GroupName="sellers",
            )
        except ClientError as e:
            return jsonify(
                error=f"Failed to add to sellers group: {e.response['Error']['Message']}"
            ), 400

        gr = _idp().admin_list_groups_for_user(
            UserPoolId=_user_pool_id(),
            Username=username,
        ).get("Groups", [])
        groups = sorted({
            str(g.get("GroupName", "")).lower()
            for g in gr
            if isinstance(g, dict) and g.get("GroupName")
        })

        _sync_role_ddb(table, user_pk, "sellers", groups)

    return jsonify(ok=True, user_pk=user_pk, type=kind, decision=decision)

@bp.get("/listing-requests")
@require_role("admin")
def admin_list_listing_requests():
    """
    List listing requests for admin review.

    Query params:
      ?status=unverified|verified|rejected|active|all (default: unverified)
      ?limit=50
    """
    status = (request.args.get("status") or "unverified").strip()

    table = _table()

    fe = Attr("SK").eq("LISTING_REQUEST")
    if status != "all":
        fe = fe & Attr("Status").eq(status)

    items: list[dict] = []
    resp = table.scan(FilterExpression=fe)
    items.extend(resp.get("Items", []))

    while resp.get("LastEvaluatedKey"):
        resp = table.scan(
            FilterExpression=fe,
            ExclusiveStartKey=resp["LastEvaluatedKey"],
        )
        items.extend(resp.get("Items", []))

    # If you want to support pagination later you can loop on LastEvaluatedKey.

    def _num(x):
        return float(x) if isinstance(x, Decimal) else x

    def to_row(it: dict) -> dict:
        return {
            "listingId": it.get("PK"),
            "sellerPk": it.get("SellerPK"),
            "devicePk": it.get("DevicePK"),
            "category": it.get("Category"),
            "brand": it.get("Brand"),
            "model": it.get("Model"),
            "variant": it.get("Variant"),
            "storage": it.get("Storage"),
            "ram": it.get("RAM"),
            "status": it.get("Status", "unverified"),
            "initialGrade": it.get("InitialGrade"),
            "initialMin": _num(it.get("InitialMin")),
            "initialMax": _num(it.get("InitialMax")),
            "finalGrade": it.get("FinalGrade"),
            "finalMin": _num(it.get("FinalMin")),
            "finalMax": _num(it.get("FinalMax")),
            "reviewRound": it.get("ReviewRound"),
            "createdAt": it.get("CreatedAt"),
            "updatedAt": it.get("UpdatedAt"),
        }

    return jsonify(items=[to_row(it) for it in items])

@bp.get("/listing-requests/<listing_id>")
@require_role("admin")
def admin_get_listing_detail(listing_id: str):
    """
    Return a single listing request with normalized fields plus Photos / Questionnaire
    for admin review.
    """
    table = _table()
    resp = table.get_item(
        Key={"PK": listing_id, "SK": "LISTING_REQUEST"},
        ConsistentRead=True,
    )
    item = resp.get("Item")
    if not item:
        return jsonify(error="Listing request not found."), 404

    def _num(x):
        return float(x) if isinstance(x, Decimal) else x

    detail = {
        "listingId": item.get("PK"),
        "sellerPk": item.get("SellerPK"),
        "devicePk": item.get("DevicePK"),
        "category": item.get("Category"),
        "brand": item.get("Brand"),
        "model": item.get("Model"),
        "variant": item.get("Variant"),
        "storage": item.get("Storage"),
        "ram": item.get("RAM"),
        "status": item.get("Status", "unverified"),
        "initialGrade": item.get("InitialGrade"),
        "initialMin": _num(item.get("InitialMin")),
        "initialMax": _num(item.get("InitialMax")),
        "finalGrade": item.get("FinalGrade"),
        "finalMin": _num(item.get("FinalMin")),
        "finalMax": _num(item.get("FinalMax")),
        "reviewRound": item.get("ReviewRound"),
        "createdAt": item.get("CreatedAt"),
        "updatedAt": item.get("UpdatedAt"),

        # raw extras used only in the detail view
        "Photos": item.get("Photos") or {},
        "Questionnaire": item.get("Questionnaire") or {},
        "ReviewReason": item.get("ReviewReason"),
    }

    return jsonify(detail)

@bp.post("/listing-requests/<listing_id>/decision")
@require_role("admin")
def admin_decide_listing(listing_id: str):
    """
    Body:
    Approve:
      {
        "decision": "approve",
        "finalGrade": "A" | "B" | "C",
        "reason": "optional notes"
      }

    Reject:
      {
        "decision": "reject",
        "reason": "required"
      }

    FinalMin/FinalMax are automatically taken from the device
    price table for the chosen grade.
    """
    body = request.get_json(silent=True) or {}
    decision = (body.get("decision") or "").strip()

    if decision not in ("approve", "reject"):
        return jsonify(error="decision must be approve|reject"), 400

    table = current_app.ddb_table
    key = {"PK": listing_id, "SK": "LISTING_REQUEST"}
    resp = table.get_item(Key=key, ConsistentRead=True)
    item = resp.get("Item")
    if not item:
        return jsonify(error="Listing request not found"), 404

    status = item.get("Status", "unverified")
    if status not in ("unverified", "verified", "rejected"):
        return jsonify(error=f"Cannot change listing in status {status!r}"), 400

    # Who is deciding?
    admin_pk = None
    u = session.get("user")
    if u:
        admin_pk = _find_user_pk_by_sub(table, u["sub"])

    now = _iso_now()
    reason = (body.get("reason") or "").strip()

    # Enforce different admin for subsequent reviews (ReviewRound > 1)
    prev_admin = item.get("ReviewedBy")
    round_ = int(item.get("ReviewRound", 1))
    if prev_admin and admin_pk and round_ > 1 and prev_admin == admin_pk:
        return jsonify(
            error="This review round must be handled by a different admin."
        ), 400

    if decision == "reject":
        if not reason:
            return jsonify(error="Reason is required when rejecting."), 400

        table.update_item(
            Key=key,
            UpdateExpression=(
                "SET #st=:rej, ReviewedAt=:ts, ReviewedBy=:adm, "
                "ReviewReason=:rs"
            ),
            ExpressionAttributeNames={"#st": "Status"},
            ExpressionAttributeValues={
                ":rej": "rejected",
                ":ts": now,
                ":adm": admin_pk,
                ":rs": reason,
            },
        )
        return jsonify(ok=True, decision="reject")

    # decision == "approve"
    final_grade = (body.get("finalGrade") or "").strip().upper()
    if final_grade not in ("A", "B", "C"):
        return jsonify(error="Final grade must be A, B, or C."), 400

    # Look up device profile to get price range for this grade
    device_pk = item.get("DevicePK")
    if not device_pk:
        return jsonify(error="Listing has no device reference."), 400

    dev_resp = table.get_item(Key={"PK": device_pk, "SK": "PROFILE"})
    device = dev_resp.get("Item")
    if not device:
        return jsonify(error="Device profile not found."), 400

    # Map grade -> min/max fields in the profile
    if final_grade == "A":
        min_key, max_key = "Grade_A_MIN", "Grade_A_MAX"
    elif final_grade == "B":
        min_key, max_key = "Grade_B_MIN", "Grade_B_MAX"
    else:  # "C"
        min_key, max_key = "Grade_C_MIN", "Grade_C_MAX"

    raw_min = device.get(min_key)
    raw_max = device.get(max_key)

    if raw_min is None or raw_max is None:
        return jsonify(
            error=f"Price range for grade {final_grade} is not configured."
        ), 400

    def to_decimal(x):
        if isinstance(x, Decimal):
            return x
        return Decimal(str(x))

    final_min_dec = to_decimal(raw_min)
    final_max_dec = to_decimal(raw_max)

    if final_min_dec > final_max_dec:
        return jsonify(error="Configured price range is invalid."), 400

    table.update_item(
        Key=key,
        UpdateExpression=(
            "SET #st=:ver, FinalGrade=:fg, FinalMin=:fmin, FinalMax=:fmax, "
            "ReviewedAt=:ts, ReviewedBy=:adm, ReviewReason=:rs"
        ),
        ExpressionAttributeNames={"#st": "Status"},
        ExpressionAttributeValues={
            ":ver": "verified",
            ":fg": final_grade,
            ":fmin": final_min_dec,
            ":fmax": final_max_dec,
            ":ts": now,
            ":adm": admin_pk,
            ":rs": reason,
        },
    )
    return jsonify(ok=True, decision="approve")


@bp.get("/orders")
@require_role("admin")
def admin_list_orders():
    """
    Admin orders = all listings that have been matched to a buyer.
    Payment is completed when PaymentStatus == "paid" and PaidAt exists.
    Query params:
      ?paymentStatus=all|pending|paid (default: all)
    """
    table = current_app.ddb_table
    payment_status = (request.args.get("paymentStatus") or "all").lower()

    filter_expr = (
        Attr("SK").eq("LISTING_REQUEST")
        & Attr("MatchedBuyerPK").exists()
    )

    if payment_status == "paid":
        filter_expr = filter_expr & Attr("PaymentStatus").eq("paid")
    elif payment_status == "pending":
        filter_expr = filter_expr & (
            Attr("PaymentStatus").ne("paid") | Attr("PaymentStatus").not_exists()
        )

    items = []
    start_key = None
    page_size = 250

    while True:
        scan_kwargs = {"FilterExpression": filter_expr, "Limit": page_size}
        if start_key:
            scan_kwargs["ExclusiveStartKey"] = start_key

        resp = table.scan(**scan_kwargs)
        items.extend(resp.get("Items", []))

        start_key = resp.get("LastEvaluatedKey")
        if not start_key:
            break

    def _as_float(val) -> float:
        try:
            return float(val)
        except Exception:
            return 0.0

    orders = []
    for it in items:
        raw_trade_price = (
            it.get("CurrentTradePrice")
            or it.get("MatchedTradePrice")
            or it.get("TradePrice")
            or 0
        )

        paid = (it.get("PaymentStatus") == "paid")
        orders.append({
            "listingId": it.get("PK"),
            "marketKey": it.get("MarketKey") or it.get("Market"),
            "sellerPk": it.get("SellerPK"),
            "buyerPk": it.get("MatchedBuyerPK"),
            "brand": it.get("Brand"),
            "model": it.get("Model"),
            "variant": it.get("Variant"),
            "grade": it.get("Grade"),
            # you said you want only pending and paid:
            "paymentStatus": "paid" if paid else "pending",
            "paidAt": it.get("PaidAt"),
            "tradePrice": _as_float(raw_trade_price),
            "listingStatus": it.get("ListingStatus") or it.get("Status"),
            "createdAt": it.get("CreatedAt"),
            "updatedAt": it.get("UpdatedAt"),
        })

    def _sort_key(o: dict) -> str:
        return o.get("paidAt") or o.get("updatedAt") or o.get("createdAt") or ""

    orders.sort(key=_sort_key, reverse=True)
    return jsonify({"ok": True, "items": orders})
