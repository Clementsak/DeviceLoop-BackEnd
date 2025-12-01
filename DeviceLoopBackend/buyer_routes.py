# DeviceLoopBackend/buyer_routes.py
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from decimal import Decimal

from flask import Blueprint, request, jsonify, current_app, session
import boto3
import json
import os
from boto3.dynamodb.conditions import Attr, Key

from .guards import require_role
from .auth_routes import _find_user_pk_by_sub, _profile_key

bp = Blueprint("buyer", __name__, url_prefix="/buyer")

now_iso = datetime.now(timezone.utc).isoformat()

FilterExpression = (
    Attr("SK").eq("LISTING_REQUEST")
    & Attr("Status").eq("active")
    & Attr("AuctionMode").eq("continuous")
    & (Attr("AuctionEndsAt").gt(now_iso) | Attr("AuctionEndsAt").not_exists())
)

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _buyer_pk_from_session() -> str | None:
    u = session.get("user")
    if not u:
        return None
    table = current_app.ddb_table
    return _find_user_pk_by_sub(table, u["sub"])


def _location_client():
    region = current_app.config.get("AWS_REGION", "ap-southeast-1")
    return boto3.client("location", region_name=region)

def _sqs_client():
  region = current_app.config.get("AWS_REGION", "ap-southeast-1")
  return boto3.client("sqs", region_name=region)

def _user_pk_from_session() -> str | None:
    """
    Generic version of _buyer_pk_from_session for any role.
    """
    u = session.get("user")
    if not u:
        return None
    table = current_app.ddb_table
    return _find_user_pk_by_sub(table, u["sub"])

@bp.post("/verify/location")
@require_role("buyers", "admin")
def submit_location_verification():
    """
    Body:
      {
        "latitude":  3.21234,
        "longitude": 101.71234
      }

    Called after browser geolocation (navigator.geolocation) on the frontend.
    We:
      - Reverse geocode with Amazon Location Service
      - Store a VERIFY#USER#ACTIVE item in DynamoDB
      - Mark profile VerifiedStatus='pending'
      - Expose it to admin via /admin/verify/queue
    """
    buyer_pk = _buyer_pk_from_session()
    if not buyer_pk:
        return ("Unauthorized", 401)

    data = request.get_json(force=True) or {}
    try:
        lat = float(data["latitude"])
        lon = float(data["longitude"])
    except (KeyError, TypeError, ValueError):
        return jsonify(error="latitude and longitude (numbers) are required"), 400

    index_name = current_app.config.get("AWS_LOCATION_INDEX", "deviceloop-place-index")
    if not index_name:
        return jsonify(error="AWS_LOCATION_INDEX is not configured on backend"), 500


    loc = _location_client()
    resp = loc.search_place_index_for_position(
        IndexName=index_name,
        Position=[lon, lat],  # NOTE: [longitude, latitude]
        MaxResults=1,
    )
    results = resp.get("Results", [])
    if not results:
        return jsonify(error="Could not resolve this position"), 400

    place = results[0]["Place"]
    country = place.get("Country")
    region = place.get("Region")
    city = place.get("Municipality")
    label = place.get("Label")

    now = _utc_now_iso()
    table = current_app.ddb_table

    # 1) Create / overwrite active verification record for this buyer
    # PK = USER#nnn, SK = VERIFY#USER#ACTIVE (fits your existing verify_decision logic)
    verify_item = {
        "PK": buyer_pk,
        "SK": "VERIFY#USER#ACTIVE",

        "Type": "VerifyUserLocation",
        "Status": "pending",
        "SubmittedAt": now,

        # Data blob shown in /admin/verify/queue
        "Data": {
            "lat": Decimal(str(lat)),
            "lon": Decimal(str(lon)),
            "country": country,
            "region": region,
            "city": city,
            "label": label,
        },

        # Put into verify queue GSI2 as "pending user" item
        "GSI2PK": "VERIFY#PENDING#user",
        "GSI2SK": now,
    }

    table.put_item(Item=verify_item)

    # 2) Mark profile as "pending" (you already have IsVerified + VerifiedStatus)
    table.update_item(
        Key=_profile_key(buyer_pk),
        UpdateExpression="SET IsVerified=:f, VerifiedStatus=:vs",
        ExpressionAttributeValues={":f": False, ":vs": "pending"},
    )

    return jsonify({
        "ok": True,
        "user_pk": buyer_pk,
        "location": {
            "latitude": lat,
            "longitude": lon,
            "country": country,
            "region": region,
            "city": city,
            "label": label,
        },
    }), 201

@bp.post("/bids")
@require_role("buyers", "admin")
def submit_bid():
    """
    Place a bid into the double auction for a given listing's market.

    Body (example):
      {
        "listingId": "LISTREQ#SELLER123#1700000000",
        "bidPrice": 3150.0,
        "buyerMin": 3000.0,   // optional, for analytics
        "buyerMax": 3400.0    // optional, for analytics
      }
    """
    buyer_pk = _buyer_pk_from_session()
    if not buyer_pk:
        return ("Unauthorized", 401)

    data = request.get_json(force=True) or {}
    listing_id = data.get("listingId")
    bid_price = data.get("bidPrice")
    buyer_min = data.get("buyerMin")
    buyer_max = data.get("buyerMax")

    if not listing_id:
        return jsonify(error="listingId is required"), 400

    try:
        bid_price = float(bid_price)
    except (TypeError, ValueError):
        return jsonify(error="bidPrice (number) is required"), 400

    if bid_price <= 0:
        return jsonify(error="bidPrice must be positive"), 400

    table = current_app.ddb_table

    # 1) Load listing to get MarketKey, AuctionMode, price envelope, and auction window
    resp = table.get_item(Key={"PK": listing_id, "SK": "LISTING_REQUEST"}, ConsistentRead=True)
    listing = resp.get("Item")
    if not listing:
        return jsonify(error="Listing not found"), 404

    status = listing.get("Status")
    if status != "active":
        return jsonify(error="Listing is not active"), 400

    # Ensure we are inside the auction window
    now = datetime.now(timezone.utc)
    starts_at_str = listing.get("AuctionStartsAt")
    ends_at_str = listing.get("AuctionEndsAt")

    try:
        if starts_at_str:
            starts_at = datetime.fromisoformat(starts_at_str)
            if now < starts_at:
                return jsonify(error="Auction has not started yet"), 400
        if ends_at_str:
            ends_at = datetime.fromisoformat(ends_at_str)
            if now > ends_at:
                return jsonify(error="Auction has already ended"), 400
    except ValueError:
        # If timestamps are malformed, be safe
        return jsonify(error="Listing has invalid auction timestamps"), 500

    # Price envelope checks (FinalMin/FinalMax)
    final_min = listing.get("FinalMin") or listing.get("InitialMin")
    final_max = listing.get("FinalMax") or listing.get("InitialMax")

    if isinstance(final_min, Decimal):
        final_min = float(final_min)
    if isinstance(final_max, Decimal):
        final_max = float(final_max)

    if final_min is not None and bid_price < final_min:
        return jsonify(error="Bid is below platform minimum for this listing"), 400
    if final_max is not None and bid_price > final_max:
        return jsonify(error="Bid is above platform maximum for this listing"), 400

    # Seller envelope check (SellerMin/SellerMax)
    seller_min = listing.get("SellerMin")
    seller_max = listing.get("SellerMax")
    if isinstance(seller_min, Decimal):
        seller_min = float(seller_min)
    if isinstance(seller_max, Decimal):
        seller_max = float(seller_max)

    if seller_min is not None and bid_price < seller_min:
        return jsonify(error="Bid is below seller minimum"), 400
    if seller_max is not None and bid_price > seller_max:
        return jsonify(error="Bid is above seller maximum"), 400

    market_key = listing.get("MarketKey")
    if not market_key:
        # Fallback: compute from DevicePK + grade
        device_pk = listing.get("DevicePK")
        grade = listing.get("FinalGrade") or listing.get("InitialGrade") or "Unknown"
        market_key = f"{device_pk}#{grade}"

    auction_mode = listing.get("AuctionMode") or "continuous"

    # 2) Create Bid item in DynamoDB
    bid_now = datetime.now(timezone.utc)
    bid_expires_at = bid_now + timedelta(hours=24)

    pk = f"MARKET#{market_key}"
    sk = f"BID#{int(bid_now.timestamp())}#{buyer_pk}"

    bid_item = {
        "PK": pk,
        "SK": sk,
        "Type": "BID",
        "MarketKey": market_key,
        "ListingPK": listing_id,
        "BuyerPK": buyer_pk,
        "BidPrice": Decimal(str(bid_price)),
        "BidStatus": "open",
        "PlacedAt": bid_now.isoformat(),
        "BidExpiresAt": bid_expires_at.isoformat(),
        "AuctionModeSnapshot": auction_mode,
        "EditCount": 0,
    }

    # Optional: store original min/max for analysis
    if buyer_min is not None:
        try:
            bid_item["BuyerMin"] = Decimal(str(float(buyer_min)))
        except (TypeError, ValueError):
            pass
    if buyer_max is not None:
        try:
            bid_item["BuyerMax"] = Decimal(str(float(buyer_max)))
        except (TypeError, ValueError):
            pass

    table.put_item(Item=bid_item)

    # 3) Send message to SQS for Lambda to process
    queue_url = current_app.config.get("BIDS_QUEUE_URL") or os.environ.get("BIDS_QUEUE_URL")
    if not queue_url:
        # In development you might want to just log and skip
        return jsonify(error="BIDS_QUEUE_URL is not configured"), 500

    sqs = _sqs_client()
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(
            {
                "type": "NEW_BID",
                "marketKey": market_key,
                "bidPk": pk,
                "bidSk": sk,
                "auctionMode": auction_mode,
            }
        ),
    )

    return jsonify(
        {
            "ok": True,
            "marketKey": market_key,
            "bidPk": pk,
            "bidSk": sk,
            "expiresAt": bid_expires_at.isoformat(),
        }
    ), 201

@bp.post("/bids/edit")
@require_role("buyers")
def edit_bid():
    """
    Allow a buyer to edit their own open bid, up to three times.
    The client sends:
    {
      "marketKey": "Device#070#C",
      "bidSk": "BID#...",
      "bidPrice": 5800,
      "buyerMin": 5600,
      "buyerMax": 6000
    }
    """
    buyer_pk = _buyer_pk_from_session()
    if not buyer_pk:
        return ("Unauthorized", 401)

    data = request.get_json() or {}
    market_key = data.get("marketKey")
    bid_sk = data.get("bidSk")
    bid_price = data.get("bidPrice")
    buyer_min = data.get("buyerMin")
    buyer_max = data.get("buyerMax")

    if not market_key or not bid_sk or bid_price is None:
        return (
            jsonify({"ok": False, "error": "Missing marketKey, bidSk or bidPrice"}),
            400,
        )

    table = current_app.ddb_table
    bid_pk = f"MARKET#{market_key}"

    # Load the bid
    resp = table.get_item(Key={"PK": bid_pk, "SK": bid_sk})
    item = resp.get("Item")
    if not item:
        return (jsonify({"ok": False, "error": "Bid not found"}), 404)

    if item.get("BuyerPK") != buyer_pk:
        return (jsonify({"ok": False, "error": "Cannot edit someone else's bid"}), 403)

    if item.get("BidStatus") != "open":
        return (jsonify({"ok": False, "error": "Only open bids can be edited"}), 400)

    now_iso = _utc_now_iso()
    bid_expires_at = item.get("BidExpiresAt")
    if bid_expires_at and bid_expires_at <= now_iso:
        return (jsonify({"ok": False, "error": "Bid has expired"}), 400)

    edit_count = int(item.get("EditCount", 0))
    if edit_count >= 3:
        return (
            jsonify({"ok": False, "error": "Edit limit reached (three edits maximum)"}),
            400,
        )

    bid_price_decimal = Decimal(str(bid_price))
    buyer_min_decimal = Decimal(str(buyer_min)) if buyer_min is not None else None
    buyer_max_decimal = Decimal(str(buyer_max)) if buyer_max is not None else None

    update_expr_parts = [
        "BidPrice = :p",
        "EditCount = :ec",
        "UpdatedAt = :now",
    ]
    expr_values = {
        ":p": bid_price_decimal,
        ":ec": edit_count + 1,
        ":now": now_iso,
    }

    if buyer_min_decimal is not None:
        update_expr_parts.append("BuyerMin = :bmin")
        expr_values[":bmin"] = buyer_min_decimal

    if buyer_max_decimal is not None:
        update_expr_parts.append("BuyerMax = :bmax")
        expr_values[":bmax"] = buyer_max_decimal

    update_expression = "SET " + ", ".join(update_expr_parts)

    table.update_item(
        Key={"PK": bid_pk, "SK": bid_sk},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expr_values,
    )

    return jsonify(
        {
            "ok": True,
            "marketKey": market_key,
            "bidPk": bid_pk,
            "bidSk": bid_sk,
            "newBidPrice": float(bid_price_decimal),
            "editCount": edit_count + 1,
        }
    )

@bp.post("/bids/cancel")
@require_role("buyers")
def cancel_bid():
    """
    Allow a buyer to cancel an open bid.
    Body:
    {
      "marketKey": "Device#070#C",
      "bidSk": "BID#..."
    }
    """
    buyer_pk = _buyer_pk_from_session()
    if not buyer_pk:
        return ("Unauthorized", 401)

    data = request.get_json() or {}
    market_key = data.get("marketKey")
    bid_sk = data.get("bidSk")

    if not market_key or not bid_sk:
        return (jsonify({"ok": False, "error": "Missing marketKey or bidSk"}), 400)

    table = current_app.ddb_table
    bid_pk = f"MARKET#{market_key}"

    resp = table.get_item(Key={"PK": bid_pk, "SK": bid_sk})
    item = resp.get("Item")
    if not item:
        return (jsonify({"ok": False, "error": "Bid not found"}), 404)

    if item.get("BuyerPK") != buyer_pk:
        return (jsonify({"ok": False, "error": "Cannot cancel someone else's bid"}), 403)

    if item.get("BidStatus") != "open":
        return (jsonify({"ok": False, "error": "Only open bids can be cancelled"}), 400)

    now_iso = _utc_now_iso()

    table.update_item(
        Key={"PK": bid_pk, "SK": bid_sk},
        UpdateExpression="SET BidStatus = :cancelled, CancelledAt = :now",
        ExpressionAttributeValues={
            ":cancelled": "cancelled",
            ":now": now_iso,
        },
    )

    return jsonify({"ok": True, "bidPk": bid_pk, "bidSk": bid_sk})


@bp.get("/listings")
@require_role("buyers", "admin")
def browse_listings():
    """
    Public buyer view of active listings.

    Optional query params (all are optional for now):
      ?category=Phones
      &brand=Samsung
      &model=Galaxy%20Flip%207
      &grade=A

    For now we do a simple table scan + in-Python filtering.
    This is OK for FYP scale.
    """
    table = current_app.ddb_table

    # 1) Get all active listing requests
    resp = table.scan(
        FilterExpression=Attr("SK").eq("LISTING_REQUEST") & Attr("Status").eq("active")
    )
    items = resp.get("Items", [])

    # 2) Read filters from query string
    q_category = request.args.get("category")
    q_brand = request.args.get("brand")
    q_model = request.args.get("model")
    q_variant = request.args.get("variant")
    q_grade = request.args.get("grade")

    filtered: list[dict] = []
    for it in items:
        if q_category and it.get("Category") != q_category:
            continue
        if q_brand and it.get("Brand") != q_brand:
            continue
        if q_model and it.get("Model") != q_model:
            continue
        if q_variant and it.get("Variant") != q_variant:
            continue
        # grade may be FinalGrade or InitialGrade depending on state
        grade = it.get("FinalGrade") or it.get("InitialGrade")
        if q_grade and grade != q_grade:
            continue
        auction_mode = it.get("AuctionMode") or "continuous"
        if auction_mode != "continuous":
            # hide interval / end_of_window on this page
            continue
        ends_at_str = it.get("AuctionEndsAt")
        if ends_at_str:
            try:
                ends_at = datetime.fromisoformat(ends_at_str)
                if ends_at <= datetime.now(timezone.utc):
                    # hide already-ended listings
                    continue
            except ValueError:
                # bad timestamp – be conservative and still show
                pass

        # Normalise numeric fields to plain floats for frontend
        def _num(x):
            return float(x) if isinstance(x, Decimal) else x

        filtered.append(
            {
                "listingId": it["PK"],
                "devicePk": it["DevicePK"],
                "category": it.get("Category"),
                "brand": it.get("Brand"),
                "model": it.get("Model"),
                "variant": it.get("Variant"),
                "storage": it.get("Storage"),
                "ram": it.get("RAM"),
                "grade": grade,
                "sellerMin": _num(it.get("SellerMin")),
                "sellerMax": _num(it.get("SellerMax")),
                "currentHighestBid": it.get("CurrentHighestBid"),
                "finalMin": _num(it.get("FinalMin") or it.get("InitialMin")),
                "finalMax": _num(it.get("FinalMax") or it.get("InitialMax")),
                "status": it.get("Status"),
                "auctionMode": auction_mode,
                "auctionStartsAt": it.get("AuctionStartsAt"),
                "auctionEndsAt": it.get("AuctionEndsAt"),
            }
        )

    return jsonify({"ok": True, "items": filtered})

@bp.get("/notifications/unread-count")
@require_role("buyers", "sellers", "admin")
def get_unread_notifications_count():
    user_pk = _user_pk_from_session()
    if not user_pk:
        return ("Unauthorized", 401)

    table = current_app.ddb_table
    notif_pk = f"NOTIF#{user_pk}"

    resp = table.query(
        KeyConditionExpression=Key("PK").eq(notif_pk),
        Limit=50,
        ScanIndexForward=False,
    )
    items = resp.get("Items", [])
    unread = sum(1 for it in items if not it.get("Read"))

    return jsonify({"ok": True, "count": unread})

@bp.get("/notifications")
@require_role("buyers", "sellers", "admin")
def list_notifications():
    user_pk = _user_pk_from_session()
    if not user_pk:
        return ("Unauthorized", 401)

    table = current_app.ddb_table
    notif_pk = f"NOTIF#{user_pk}"

    resp = table.query(
        KeyConditionExpression=Key("PK").eq(notif_pk),
        Limit=50,
        ScanIndexForward=False,
    )
    items = resp.get("Items", [])

    # Small projection for the front end
    projected = []
    for it in items:
        projected.append(
            {
                "pk": it["PK"],
                "sk": it["SK"],
                "type": it.get("Type"),
                "userRole": it.get("UserRole"),
                "listingPk": it.get("ListingPK"),
                "devicePk": it.get("DevicePK"),
                "marketKey": it.get("MarketKey"),
                "tradePrice": float(it.get("TradePrice", 0)),
                "createdAt": it.get("CreatedAt"),
                "read": bool(it.get("Read")),
            }
        )

    return jsonify({"ok": True, "items": projected})

@bp.post("/notifications/mark-all-read")
@require_role("buyers", "sellers", "admin")
def mark_all_notifications_read():
    user_pk = _user_pk_from_session()
    if not user_pk:
        return ("Unauthorized", 401)

    table = current_app.ddb_table
    notif_pk = f"NOTIF#{user_pk}"

    resp = table.query(
        KeyConditionExpression=Key("PK").eq(notif_pk),
        Limit=50,
        ScanIndexForward=False,
    )
    items = resp.get("Items", [])

    now_iso = _utc_now_iso()
    for it in items:
        if it.get("Read"):
            continue
        table.update_item(
            Key={"PK": it["PK"], "SK": it["SK"]},
            UpdateExpression="SET #read = :true, ReadAt = :now",
            ExpressionAttributeNames={"#read": "Read"},
            ExpressionAttributeValues={":true": True, ":now": now_iso},
        )

    return jsonify({"ok": True})

@bp.get("/my-bids")
@require_role("buyers", "admin")
def get_my_bids():
    """
    Return a summary of all bids placed by the currently logged-in buyer.

    Response shape matches MyBidSummary in the frontend:
      {
        "items": [
          {
            "bidSk": "...",
            "marketKey": "...",
            "deviceLabel": "...",
            "grade": "A",
            "finalBidPrice": 1234.56,
            "buyerMin": 1200.0,
            "buyerMax": 1300.0,
            "status": "open",
            "createdAt": "ISO",
            "updatedAt": "ISO | null",
            "remainingEdits": 2,
            "matchedListingId": "LISTREQ#...",
            "matchedTradePrice": 1200.0
          }
        ]
      }
    """
    buyer_pk = _buyer_pk_from_session()
    if not buyer_pk:
        return ("Unauthorized", 401)

    table = current_app.ddb_table

    # --------- 1) Scan for this buyer's bids ---------
    scan_kwargs = {
        "FilterExpression": Attr("Type").eq("BID") & Attr("BuyerPK").eq(buyer_pk),
    }

    bid_items: list[dict] = []
    while True:
        resp = table.scan(**scan_kwargs)
        bid_items.extend(resp.get("Items", []) or [])
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
        scan_kwargs["ExclusiveStartKey"] = last_key

    # Early exit
    if not bid_items:
        return jsonify({"items": []})

    # --------- 2) Helper to normalise Decimal ---------
    from decimal import Decimal as _Dec

    def _num(v):
        if isinstance(v, _Dec):
            return float(v)
        return v

    # --------- 3) Pre-load related listings & devices ---------
    # Collect unique ListingPKs
    listing_pks = {it.get("ListingPK") for it in bid_items if it.get("ListingPK")}
    listings_by_pk: dict[str, dict] = {}

    for pk in listing_pks:
        try:
            resp = table.get_item(Key={"PK": pk, "SK": "LISTING_REQUEST"})
            item = resp.get("Item")
            if item:
                listings_by_pk[pk] = item
        except Exception as e:
            current_app.logger.warning("Failed to load listing %s: %s", pk, e)

    # Collect unique DevicePKs from listings
    device_pks = {
        it.get("DevicePK")
        for it in listings_by_pk.values()
        if it.get("DevicePK")
    }
    devices_by_pk: dict[str, dict] = {}
    for dpk in device_pks:
        try:
            resp = table.get_item(Key={"PK": dpk, "SK": "PROFILE"})
            item = resp.get("Item")
            if item:
                devices_by_pk[dpk] = item
        except Exception as e:
            current_app.logger.warning("Failed to load device %s: %s", dpk, e)

    # --------- 4) Build MyBidSummary objects ---------
    results: list[dict] = []

    for it in bid_items:
        bid_sk = it.get("SK", "")
        market_key = it.get("MarketKey") or ""
        listing_pk = it.get("ListingPK")

        listing = listings_by_pk.get(listing_pk or "", {})
        device_pk = listing.get("DevicePK")
        device = devices_by_pk.get(device_pk or "", {})

        # Device label
        brand = device.get("Brand") or device.get("brand") or ""
        model = device.get("Model") or device.get("model") or ""
        storage = device.get("Storage") or device.get("storage") or ""
        ram = device.get("RAM") or device.get("ram") or ""

        label = (brand + " " + model).strip() or (market_key or "Unknown device")
        if storage or ram:
            extra = " / ".join([x for x in [storage, ram] if x])
            if extra:
                label = f"{label} ({extra})"

        # Grade
        grade = (
            listing.get("FinalGrade")
            or listing.get("InitialGrade")
            or None
        )
        if not grade and market_key:
            parts = str(market_key).split("#")
            if len(parts) >= 3:
                grade = parts[-1]

        bid_price = _num(it.get("BidPrice"))
        buyer_min = _num(it.get("BuyerMin")) if it.get("BuyerMin") is not None else None
        buyer_max = _num(it.get("BuyerMax")) if it.get("BuyerMax") is not None else None

        status = it.get("BidStatus") or "open"
        created_at = it.get("PlacedAt") or it.get("CreatedAt") or ""
        updated_at = (
            it.get("UpdatedAt") or it.get("MatchedAt") or it.get("PlacedAt") or None
        )

        edit_count_raw = it.get("EditCount") or 0
        try:
            edit_count = int(edit_count_raw)
        except Exception:
            edit_count = 0
        remaining_edits = max(0, 3 - edit_count)

        matched_trade_price = _num(it.get("MatchedTradePrice")) \
            if it.get("MatchedTradePrice") is not None else None

        matched_listing_id = listing_pk if matched_trade_price is not None or status in (
            "filled",
            "matched",
            "partially_filled",
        ) else None

        results.append(
            {
                "bidSk": bid_sk,
                "marketKey": market_key,
                "deviceLabel": label,
                "grade": grade,
                "finalBidPrice": bid_price,
                "buyerMin": buyer_min,
                "buyerMax": buyer_max,
                "status": status,
                "createdAt": created_at,
                "updatedAt": updated_at,
                "remainingEdits": remaining_edits,
                "matchedListingId": matched_listing_id,
                "matchedTradePrice": matched_trade_price,
            }
        )

    # Sort newest first (PlacedAt descending)
    results.sort(key=lambda b: b.get("createdAt") or "", reverse=True)

    return jsonify({"items": results})
