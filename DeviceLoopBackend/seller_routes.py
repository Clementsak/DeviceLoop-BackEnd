# DeviceLoopBackend/seller_routes.py
from __future__ import annotations
import os, uuid
from datetime import datetime, timezone, timedelta

from flask import Blueprint, request, jsonify, current_app, session
import boto3
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal

from .guards import require_role
from .auth_routes import _find_user_pk_by_sub, _profile_key
from .grading import compute_initial_grade_and_range, GradeRejected

bp = Blueprint("seller", __name__, url_prefix="/seller")

def _utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def _seller_pk_from_session():
    """Resolve the user's profile PK (used as seller id) from session.sub."""
    u = session.get("user")
    if not u:
        return None
    table = current_app.ddb_table
    pk = _find_user_pk_by_sub(table, u["sub"])
    return pk

def _listing_key(seller_pk: str, listing_id: str):
    return {"PK": f"SELLER#{seller_pk}", "SK": f"LISTING#{listing_id}"}


# ---------- Dashboard summary ----------
@bp.get("/summary")
@require_role("sellers", "admin")
def seller_summary():
    seller_pk = _seller_pk_from_session()
    if not seller_pk:
        return ("Unauthorized", 401)

    table = current_app.ddb_table

    # TODO: Replace with real queries
    data = {
        "listings_active": 0,
        "orders_pending": 0,
        "messages_unread": 0,
        "next_payout": {"amount": 0, "date": None},
        "generated_at": _utc_now_iso(),
    }
    return jsonify(data)

def _table():
    """Return the shared DynamoDB table."""
    return current_app.ddb_table


@bp.post("/listing-requests")
@require_role("sellers", "admin")
def create_listing_request():
    """
    Create a new listing request for the selected catalogue device.

    Request JSON (what the React modal sends):

    {
      "device": { "pk": "Device#088" },
      "photos": {
        "front":  "listings/....-front.jpg",
        "back":   "listings/....-back.jpg",
        "left":   "listings/....-left.jpg",
        "right":  "listings/....-right.jpg",
        "top":    "listings/....-top.jpg",
        "bottom": "listings/....-bottom.jpg",
        "imei":   "listings/....-imei.jpg"
      },
      "questionnaire": {
        "freeOfLocks": true,
        "canPowerOn": true,
        "screenCondition": "Flawless",
        "bodyCondition": "Flawless",
        "biometric": "yes",
        "cameras": "front_issue",
        "coreFunctions": "ok",
        "seriousIssues": []
      }
    }
    """

    data = request.get_json(force=True) or {}

    device_info = data.get("device") or {}
    device_pk = device_info.get("pk")
    photos = data.get("photos") or {}
    questionnaire = data.get("questionnaire") or {}

    if not device_pk:
        return jsonify({"error": "device is required."}), 400

    table = _table()

    # Look up the PROFILE row
    resp = table.get_item(Key={"PK": device_pk, "SK": "PROFILE"})
    device = resp.get("Item")

    if not device or not device.get("Active", True):
        return jsonify({"error": "Selected device is not recognised by the platform."}), 400

    # Compute grade + platform price range
    try:
        initial_grade, initial_min, initial_max = compute_initial_grade_and_range(
            device, questionnaire
        )
    except GradeRejected as e:
        return jsonify({"error": str(e)}), 400

    # Seller PK from session (Cognito subject, mapped in auth_routes)
    seller_pk = _seller_pk_from_session()
    if not seller_pk:
        return ("Unauthorized", 401)

    now = datetime.now(timezone.utc).isoformat()
    # PK pattern: LISTREQ#<seller>#<timestamp>
    ts = int(datetime.now(timezone.utc).timestamp())
    listing_pk = f"LISTREQ#{seller_pk}#{ts}"

    item = {
        "PK": listing_pk,
        "SK": "LISTING_REQUEST",
        "SellerPK": seller_pk,
        "DevicePK": device_pk,

        # Snapshot of device summary so we can render tables without another join
        "Category": device.get("Category"),
        "Brand": device.get("Brand"),
        "Model": device.get("Model"),
        "Variant": device.get("Variant"),
        "Storage": device.get("Storage"),
        "RAM": device.get("RAM"),

        "InitialGrade": initial_grade,
        "InitialMin": Decimal(str(initial_min)),
        "InitialMax": Decimal(str(initial_max)),

        "Status": "unverified",
        "ReviewRound": 1,

        # store photos as a map so admin can see each side
        "Photos": {
            "Front": photos.get("front"),
            "Back": photos.get("back"),
            "Left": photos.get("left"),
            "Right": photos.get("right"),
            "Top": photos.get("top"),
            "Bottom": photos.get("bottom"),
            "IMEI": photos.get("imei"),
        },

        # raw answers for audit
        "Questionnaire": questionnaire,
        "CreatedAt": now,
        "UpdatedAt": now,
    }

    table.put_item(Item=item)

    return (
        jsonify(
            {
                "listingId": listing_pk,
                "initialGrade": initial_grade,
                "initialMin": initial_min,
                "initialMax": initial_max,
                "status": "unverified",
            }
        ),
        201,
    )


@bp.post("/listing-requests/<listing_id>/cancel")
@require_role("sellers", "admin")
def cancel_listing_request(listing_id):
    seller_pk = _seller_pk_from_session()
    if not seller_pk:
        return ("Unauthorized", 401)

    table = _table()
    key = {"PK": listing_id, "SK": "LISTING_REQUEST"}
    resp = table.get_item(Key=key, ConsistentRead=True)
    item = resp.get("Item")
    if not item or item.get("SellerPK") != seller_pk:
        return ("Not found", 404)

    status = item.get("Status", "unverified")
    if status  in ("active", "ended"):
        return jsonify({"error": "Cannot cancel an active or ended listing."}), 400

    table.update_item(
        Key=key,
        UpdateExpression="SET #st = :cancelled, UpdatedAt = :now",
        ExpressionAttributeNames={"#st": "Status"},
        ExpressionAttributeValues={
            ":cancelled": "cancelled",
            ":now": _utc_now_iso(),
        },
    )
    return jsonify({"ok": True})


@bp.post("/listing-requests/<listing_id>/request-review")
@require_role("sellers", "admin")
def request_additional_review(listing_id):
    seller_pk = _seller_pk_from_session()
    if not seller_pk:
        return ("Unauthorized", 401)

    table = current_app.ddb_table
    key = {"PK": listing_id, "SK": "LISTING_REQUEST"}
    resp = table.get_item(Key=key, ConsistentRead=True)
    item = resp.get("Item")
    if not item or item.get("SellerPK") != seller_pk:
        return ("Not found", 404)

    round_ = int(item.get("ReviewRound", 1))
    status = item.get("Status")

    if status not in ("verified", "rejected"):
        return jsonify(
            {"error": "You can only request another review after a verified or rejected decision."}
        ), 400
    if round_ >= 3:
        return jsonify({"error": "You have reached the maximum review rounds."}), 400

    table.update_item(
        Key=key,
        UpdateExpression=(
            "SET ReviewRound = :r, #st = :unverified, UpdatedAt = :now"
        ),
        ExpressionAttributeNames={"#st": "Status"},
        ExpressionAttributeValues={
            ":r": round_ + 1,
            ":unverified": "unverified",
            ":now": _utc_now_iso(),
        },
    )
    return jsonify({"ok": True, "newRound": round_ + 1})



@bp.post("/listing-requests/<listing_id>/accept")
@require_role("sellers", "admin")
def accept_grade_and_activate(listing_id):
    seller_pk = _seller_pk_from_session()
    if not seller_pk:
        return ("Unauthorized", 401)

    body = request.get_json(force=True) or {}
    seller_min = body.get("sellerMin")
    seller_max = body.get("sellerMax")
    duration = int(body.get("durationHours") or 0)  # 6, 12, 24, 72

    if duration not in (6, 12, 24, 72):
        return jsonify({"error": "Invalid duration."}), 400

    if seller_min is None or seller_max is None:
        return jsonify({"error": "sellerMin and sellerMax are required."}), 400
    if not (isinstance(seller_min, (int, float)) and isinstance(seller_max, (int, float))):
        return jsonify({"error": "Prices must be numbers."}), 400

    table = _table()
    key = {"PK": listing_id, "SK": "LISTING_REQUEST"}
    resp = table.get_item(Key=key, ConsistentRead=True)
    item = resp.get("Item")
    if not item or item.get("SellerPK") != seller_pk:
        return ("Not found", 404)

    if item.get("Status") != "verified":
        return jsonify({"error": "Listing must be verified before activation."}), 400

    final_min = item.get("FinalMin") or item.get("InitialMin")
    final_max = item.get("FinalMax") or item.get("InitialMax")

     # Ensure numbers
    if isinstance(final_min, Decimal):
        final_min = float(final_min)
    if isinstance(final_max, Decimal):
        final_max = float(final_max)
    if seller_min < final_min or seller_max > final_max or seller_min > seller_max:
        return jsonify({"error": "Seller price range must be within verified range and valid."}), 400

    now = datetime.now(timezone.utc)
    start = now.isoformat()
    end = (now + timedelta(hours=duration)).isoformat()

    table.update_item(
        Key=key,
        UpdateExpression=(
            "SET SellerMin = :smin, SellerMax = :smax, DurationHours = :dur, "
            "AuctionStartsAt = :start, AuctionEndsAt = :end, "
            "#st = :active, UpdatedAt = :now"
        ),
        ExpressionAttributeNames={"#st": "Status"},
        ExpressionAttributeValues={
            ":smin": seller_min,
            ":smax": seller_max,
            ":dur": duration,
            ":start": start,
            ":end": end,
            ":active": "active",
            ":now": start,
        },
    )
    return jsonify({"ok": True})

@bp.get("/listings/<listing_id>")
@require_role("sellers", "admin")
def get_listing(listing_id):
    seller_pk = _seller_pk_from_session()
    if not seller_pk:
        return ("Unauthorized", 401)

    table =  _table()
    key = {"PK": listing_id, "SK": "LISTING_REQUEST"}
    resp = table.get_item(Key=key, ConsistentRead=True)
    item = resp.get("Item")
    if not item or item.get("SellerPK") != seller_pk:
        return ("Not found", 404)
    return jsonify(item)

@bp.put("/listings/<listing_id>")
@require_role("sellers", "admin")
def update_listing(listing_id):
    seller_pk = _seller_pk_from_session()
    if not seller_pk:
        return ("Unauthorized", 401)

    data = request.get_json(force=True) or {}
    data["UpdatedAt"] = _utc_now_iso()

    table = _table()
    key = {"PK": listing_id, "SK": "LISTING_REQUEST"}

    # For simplicity, overwrite (you can switch to UpdateExpression later)
    resp = table.get_item(Key=key, ConsistentRead=True)
    item = resp.get("Item")
    if not item or item.get("SellerPK") != seller_pk:
        return ("Not found", 404)

    for k, v in data.items():
        if k in {"PK", "SK", "SellerPK"}:
            continue
        item[k] = v

    table.put_item(Item=item)
    return jsonify({"ok": True})

@bp.delete("/listings/<listing_id>")
@require_role("sellers", "admin")
def delete_listing(listing_id):
    seller_pk = _seller_pk_from_session()
    if not seller_pk:
        return ("Unauthorized", 401)

    table = _table()
    key = {"PK": listing_id, "SK": "LISTING_REQUEST"}
    resp = table.get_item(Key=key, ConsistentRead=True)
    item = resp.get("Item")
    if not item or item.get("SellerPK") != seller_pk:
        return ("Not found", 404)

    table.delete_item(Key=key)
    return jsonify({"ok": True})

# ---------- Orders (seller view) ----------
@bp.get("/orders")
@require_role("sellers", "admin")
def list_orders():
    seller_pk = _seller_pk_from_session()
    if not seller_pk:
        return ("Unauthorized", 401)

    status = request.args.get("status")  # optional filter
    page_size = min(int(request.args.get("limit", "25")), 100)

    table = current_app.ddb_table
    # TODO: Query GSI by SellerId if your orders are keyed by order PK
    items = []
    return jsonify({"items": items})

@bp.post("/orders/<order_id>/ship")
@require_role("sellers", "admin")
def mark_shipped(order_id):
    seller_pk = _seller_pk_from_session()
    if not seller_pk:
        return ("Unauthorized", 401)

    data = request.get_json(force=True) or {}
    tracking = data.get("tracking")
    carrier = data.get("carrier")
    if not tracking or not carrier:
        return jsonify({"error": "tracking and carrier are required"}), 400

    # TODO: Update order item with shipment details.
    return jsonify({"ok": True})

# ---------- Payouts ----------
@bp.get("/payouts")
@require_role("sellers", "admin")
def list_payouts():
    seller_pk = _seller_pk_from_session()
    if not seller_pk:
        return ("Unauthorized", 401)

    table = current_app.ddb_table
    # TODO: Query payouts by SellerId
    items = []
    return jsonify({"items": items})

# ---------- Settings ----------
@bp.get("/settings")
@require_role("sellers", "admin")
def get_settings():
    seller_pk = _seller_pk_from_session()
    if not seller_pk:
        return ("Unauthorized", 401)

    table = current_app.ddb_table
    # Example: store settings under PROFILE item
    resp = table.get_item(Key=_profile_key(seller_pk), ConsistentRead=True)
    prof = resp.get("Item", {})
    settings = prof.get("SellerSettings", {})  # nested doc
    return jsonify(settings)

@bp.put("/settings")
@require_role("sellers", "admin")
def put_settings():
    seller_pk = _seller_pk_from_session()
    if not seller_pk:
        return ("Unauthorized", 401)

    table = current_app.ddb_table
    resp = table.get_item(Key=_profile_key(seller_pk), ConsistentRead=True)
    prof = resp.get("Item") or {}
    prof.setdefault("SellerSettings", {}).update(request.get_json(force=True) or {})
    table.put_item(Item=prof)
    return jsonify({"ok": True})

@bp.get("/listing-requests")
@require_role("sellers", "admin")
def list_listing_requests():
    seller_pk = _seller_pk_from_session()
    if not seller_pk:
        return ("Unauthorized", 401)

    table = _table()

    # Same scan you had before: all LISTING_REQUEST items for this seller
    fe = Attr("SK").eq("LISTING_REQUEST") & Attr("SellerPK").eq(seller_pk)

    items: list[dict] = []
    resp = table.scan(FilterExpression=fe)
    items.extend(resp.get("Items", []))
    while resp.get("LastEvaluatedKey"):
        resp = table.scan(
            FilterExpression=fe,
            ExclusiveStartKey=resp["LastEvaluatedKey"],
        )
        items.extend(resp.get("Items", []))

    def _num(x):
        return float(x) if isinstance(x, Decimal) else x

    def to_listing(it: dict) -> dict:
        return {
            "ListingId": it.get("PK"),
            "Category": it.get("Category"),
            "Brand": it.get("Brand"),
            "Model": it.get("Model"),
            "Variant": it.get("Variant"),
            "Storage": it.get("Storage"),
            "RAM": it.get("RAM"),
            "Status": it.get("Status", "unverified"),
            "InitialGrade": it.get("InitialGrade"),
            "InitialMin": _num(it.get("InitialMin")),
            "InitialMax": _num(it.get("InitialMax")),
            "FinalGrade": it.get("FinalGrade"),
            "FinalMin": _num(it.get("FinalMin")),
            "FinalMax": _num(it.get("FinalMax")),
            "ReviewRound": it.get("ReviewRound"),
            "ReviewReason": it.get("ReviewReason"),
            "SellerMin": _num(it.get("SellerMin")),
            "SellerMax": _num(it.get("SellerMax")),
            "DurationHours": _num(it.get("DurationHours")),
            "AuctionStartsAt": it.get("AuctionStartsAt"),
            "AuctionEndsAt": it.get("AuctionEndsAt"),
            "CurrentHighestBid": _num(it.get("CurrentHighestBid")),
            "CurrentHighestBidderPK": it.get("CurrentHighestBidderPK"),
        }

    # First map all Dynamo items into the shape expected by the frontend
    rows = [to_listing(it) for it in items]

    # Then split into ongoing listings vs requests
    requests: list[dict] = []
    listings: list[dict] = []

    for row in rows:
        status = (row.get("Status") or "").lower()

        # Treat active (and optionally ended) as "ongoing listings"
        if status in ("active", "ended"):
            listings.append(row)
        else:
            requests.append(row)

    return jsonify({
        "requests": requests,
        "listings": listings,
    })
