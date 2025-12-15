# DeviceLoopBackend/buyer_routes.py
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from decimal import Decimal

from flask import Blueprint, request, jsonify, current_app, session
import boto3
import json
import os
from boto3.dynamodb.conditions import Attr, Key
from .s3_utils import presign_get
import urllib.parse
from botocore.exceptions import ClientError
import re
from .guards import require_role
from .auth_routes import _find_user_pk_by_sub, _profile_key

bp = Blueprint("buyer", __name__, url_prefix="/buyer")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")
sns_client = boto3.client("sns") if SNS_TOPIC_ARN else None

now_iso = datetime.now(timezone.utc).isoformat()

FilterExpression = (
    Attr("SK").eq("LISTING_REQUEST")
    & Attr("Status").eq("active")
    & Attr("AuctionMode").eq("continuous")
    & (Attr("AuctionEndsAt").gt(now_iso) | Attr("AuctionEndsAt").not_exists())
)

EMAIL_FROM = os.environ.get("EMAIL_FROM")
AWS_REGION = os.environ.get("AWS_REGION", "ap-southeast-1")

simple_email_service_client = boto3.client("ses", region_name=AWS_REGION)

def _ddb_get_user_email(table, user_pk: str | None) -> str | None:
    if not user_pk:
        return None
    resp = table.get_item(Key={"PK": user_pk, "SK": "PROFILE"})
    item = resp.get("Item") or {}
    # adjust this key if your PROFILE uses a different attribute name
    return item.get("Email") or item.get("email")

def _send_email(to_addr: str | None, subject: str, body: str) -> None:
    if not to_addr:
        print(f"[EMAIL] skip: no recipient (subject={subject})")
        return
    if not EMAIL_FROM:
        print("[EMAIL] skip: EMAIL_FROM is not set")
        return

    try:
        resp = simple_email_service_client.send_email(
            Source=EMAIL_FROM,
            Destination={"ToAddresses": [to_addr]},
            Message={
                "Subject": {"Data": subject, "Charset": "UTF-8"},
                "Body": {"Text": {"Data": body, "Charset": "UTF-8"}},
            },
        )
        print(
            f"[EMAIL] sent ok to={to_addr} subject={subject} messageId={resp.get('MessageId')}"
        )
    except Exception as exc:
        # never break payment flow because email failed
        print(f"[EMAIL] send failed to={to_addr} subject={subject} error={exc}")

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
    Create a *market-level* bid for a given device + grade.

    This is called by the BidWizardModal.

    Expected JSON body (from the wizard):

      {
        "devicePk": "DEVICE#083A",
        "grade": "A",
        "mode": "interval" | "end_of_window" | "continuous",
        "buyerMin": 6200.0,
        "buyerMax": 6500.0,
        "finalBid": 6300.0,
        "bandLow": 6200.0,
        "bandHigh": 6300.0,
        "isBuyout": true | false
      }

    Notes:
      * We do NOT tie the bid to a specific listing anymore.
      * We still send a NEW_BID message to SQS so your existing
        deviceloop-bids-matcher / deviceloop-bids-clearing Lambdas
        keep working with the same message shape.
    """
    buyer_pk = _buyer_pk_from_session()
    if not buyer_pk:
        return ("Unauthorized", 401)

    data = request.get_json(silent=True) or {}

    device_pk = data.get("devicePk")
    grade = data.get("grade")
    mode = data.get("mode") or "continuous"  # default for wizard

    if not device_pk:
        return jsonify(error="devicePk is required"), 400
    if not grade:
        return jsonify(error="grade is required"), 400

    # ----- parse numeric fields -----
    def _parse_float(val, field_name, required=False):
        if val is None:
            if required:
                raise ValueError(f"{field_name} is required")
            return None
        try:
            return float(val)
        except (TypeError, ValueError):
            raise ValueError(f"{field_name} must be a number")

    try:
        bid_price = _parse_float(data.get("finalBid") or data.get("bidPrice"), "finalBid", required=True)
        buyer_min = _parse_float(data.get("buyerMin"), "buyerMin")
        buyer_max = _parse_float(data.get("buyerMax"), "buyerMax")
        band_low = _parse_float(data.get("bandLow"), "bandLow")
        band_high = _parse_float(data.get("bandHigh"), "bandHigh")
    except ValueError as e:
        return jsonify(error=str(e)), 400

    if bid_price <= 0:
        return jsonify(error="finalBid must be positive"), 400

    # Basic sanity checks inside the user's envelope (frontend already
    # enforces platform range + 20% band logic).
    if buyer_min is not None and buyer_max is not None and buyer_min > buyer_max:
        return jsonify(error="buyerMin must be <= buyerMax"), 400

    if buyer_min is not None and bid_price < buyer_min:
        return jsonify(error="finalBid must be >= buyerMin"), 400

    if buyer_max is not None and bid_price > buyer_max:
        return jsonify(error="finalBid must be <= buyerMax"), 400

    is_buyout = bool(data.get("isBuyout"))

    table = current_app.ddb_table

    # ----- build MarketKey consistent with existing code -----
    # DevicePK is like "DEVICE#083A", so MarketKey becomes "DEVICE#083A#A"
    market_key = f"{device_pk}#{grade}"

    bid_now = datetime.now(timezone.utc)
    bid_expires_at = bid_now + timedelta(hours=24)

    pk = f"MARKET#{market_key}"
    sk = f"BID#{int(bid_now.timestamp())}#{buyer_pk}"

    from decimal import Decimal as _Dec

    bid_item = {
        "PK": pk,
        "SK": sk,
        "Type": "BID",
        "MarketKey": market_key,
        "BuyerPK": buyer_pk,
        "DevicePK": device_pk,
        "Grade": grade,
        "BidPrice": _Dec(str(bid_price)),
        "BuyerMin": _Dec(str(buyer_min)) if buyer_min is not None else None,
        "BuyerMax": _Dec(str(buyer_max)) if buyer_max is not None else None,
        "BandLow": _Dec(str(band_low)) if band_low is not None else None,
        "BandHigh": _Dec(str(band_high)) if band_high is not None else None,
        "IsBuyout": is_buyout,
        "BidStatus": "open",
        "AuctionModeSnapshot": mode,
        "PlacedAt": bid_now.isoformat(),
        "BidExpiresAt": bid_expires_at.isoformat(),
        "EditCount": 0,
    }

    # Remove None fields so DynamoDB is happy
    bid_item = {k: v for k, v in bid_item.items() if v is not None}

    table.put_item(Item=bid_item)

    # ----- push NEW_BID to SQS (same structure as before) -----
    queue_url = current_app.config.get("BIDS_QUEUE_URL") or os.environ.get(
        "BIDS_QUEUE_URL"
    )
    if not queue_url:
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
                "auctionMode": mode,
            }
        ),
    )

    return (
        jsonify(
            {
                "ok": True,
                "marketKey": market_key,
                "bidPk": pk,
                "bidSk": sk,
                "expiresAt": bid_expires_at.isoformat(),
            }
        ),
        201,
    )


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

        # ✅ NEW: extract the "front" photo key and presign it
        photos = it.get("Photos") or {}
        # new var: photo_front_key = the raw S3 key, taken from the "Front" label (case-insensitive)
        photo_front_key = (
            photos.get("Front")
            or photos.get("front")
            or photos.get("FRONT")
        )
        # new var: thumbnail_url = temporary HTTPS URL generated by backend for that S3 key
        thumbnail_url = presign_get(photo_front_key) if photo_front_key else None

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
                "auctionMode": it.get("AuctionMode"),
                "auctionStartsAt": it.get("AuctionStartsAt"),
                "auctionEndsAt": it.get("AuctionEndsAt"),
                "thumbnailUrl": thumbnail_url,
            }
        )

    return jsonify({"ok": True, "items": filtered})

@bp.get("/markets")
@require_role("buyers", "sellers", "admin")
def list_device_markets():
    """
    Aggregate view of active markets (device + grade).

    Optional query parameters:
      ?category=...
      &brand=...
      &model=...
      &grade=A

    Returns one row per MarketKey with:
      - counts of active listings per auction mode
      - seller price range across listings
      - platform range and release info from PROFILE row
      - number of open bids and distinct bidders
    """
    table = current_app.ddb_table

    # === 1) Active listings (LISTING_REQUEST) ===
    resp = table.scan(
        FilterExpression=Attr("SK").eq("LISTING_REQUEST") & Attr("Status").eq("active")
    )
    listing_items = resp.get("Items", []) or []

    q_category = request.args.get("category")
    q_brand = request.args.get("brand")
    q_model = request.args.get("model")
    q_grade = (request.args.get("grade") or "").upper()

    def _num(v):
        if isinstance(v, Decimal):
            return float(v)
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    markets: dict[str, dict] = {}

    # Cache for PROFILE lookups
    profile_cache: dict[tuple[str, str], dict] = {}

    def get_profile_meta(device_pk: str, grade: str) -> dict:
        key = (device_pk, grade)
        if key in profile_cache:
            return profile_cache[key]

        meta = {
            "platformMin": None,
            "platformMax": None,
            "releasePrice": None,
            "releaseDate": None,
        }

        try:
            resp = table.get_item(Key={"PK": device_pk, "SK": "PROFILE"})
            prof = resp.get("Item") or {}
        except Exception:
            prof = {}

        if prof:
            if grade == "A":
                min_v = prof.get("Grade_A_MIN")
                max_v = prof.get("Grade_A_MAX")
            elif grade == "B":
                min_v = prof.get("Grade_B_MIN")
                max_v = prof.get("Grade_B_MAX")
            elif grade == "C":
                min_v = prof.get("Grade_C_MIN")
                max_v = prof.get("Grade_C_MAX")
            else:
                min_v = max_v = None

            meta["platformMin"] = _num(min_v)
            meta["platformMax"] = _num(max_v)
            meta["releasePrice"] = _num(prof.get("ReleasePrice"))
            meta["releaseDate"] = prof.get("ReleaseDate")

        profile_cache[key] = meta
        return meta

    now = datetime.now(timezone.utc)

    # First pass: aggregate listings
    for it in listing_items:
        if q_category and it.get("Category") != q_category:
            continue
        if q_brand and it.get("Brand") != q_brand:
            continue
        if q_model and it.get("Model") != q_model:
            continue

        grade = (it.get("FinalGrade") or it.get("InitialGrade") or "").upper()
        if q_grade and grade != q_grade:
            continue

        # Drop listings that have already ended, even if Status is still "active"
        ends_at_str = it.get("AuctionEndsAt")
        if ends_at_str:
            try:
                ends_at = datetime.fromisoformat(ends_at_str)
                if ends_at <= now:
                    continue
            except Exception:
                # If invalid timestamp, keep the listing rather than hiding silently
                pass

        market_key = it.get("MarketKey")
        device_pk = it.get("DevicePK")
        if not market_key or not device_pk or not grade:
            continue

        auction_mode = (it.get("AuctionMode") or "continuous").lower()

        if market_key not in markets:
            meta = (
                get_profile_meta(device_pk, grade)
                if device_pk and grade
                else {
                    "platformMin": None,
                    "platformMax": None,
                    "releasePrice": None,
                    "releaseDate": None,
                }
            )

            markets[market_key] = {
                "marketKey": market_key,
                "devicePk": device_pk,
                "category": it.get("Category"),
                "brand": it.get("Brand"),
                "model": it.get("Model"),
                "variant": it.get("Variant"),
                "storage": it.get("Storage"),
                "ram": it.get("RAM"),
                "grade": grade,
                "numListings": 0,
                "numContinuous": 0,
                "numInterval": 0,
                "numEndOfWindow": 0,
                "sellerRangeMin": None,
                "sellerRangeMax": None,
                "platformMin": meta["platformMin"],
                "platformMax": meta["platformMax"],
                "releasePrice": meta["releasePrice"],
                "releaseDate": meta["releaseDate"],
                # will be filled in second pass
                "numBids": 0,
                "numBidders": 0,
            }

        m = markets[market_key]
        m["numListings"] += 1
        if auction_mode == "continuous":
            m["numContinuous"] += 1
        elif auction_mode == "interval":
            m["numInterval"] += 1
        elif auction_mode == "end_of_window":
            m["numEndOfWindow"] += 1

        seller_min = _num(it.get("SellerMin"))
        seller_max = _num(it.get("SellerMax"))
        if seller_min is not None:
            if m["sellerRangeMin"] is None or seller_min < m["sellerRangeMin"]:
                m["sellerRangeMin"] = seller_min
        if seller_max is not None:
            if m["sellerRangeMax"] is None or seller_max > m["sellerRangeMax"]:
                m["sellerRangeMax"] = seller_max

    # === 2) Open bids per market ===
    bids_resp = table.scan(
        FilterExpression=Attr("Type").eq("BID") & Attr("BidStatus").eq("open")
    )
    bid_items = bids_resp.get("Items", []) or []

    bidders_by_market: dict[str, set] = {}

    for bid in bid_items:
        market_key = bid.get("MarketKey")
        if not market_key:
            continue

        # Only care about bids for markets that have listings
        m = markets.get(market_key)
        if not m:
            continue

        # Ignore expired bids
        expires_at_str = bid.get("BidExpiresAt")
        if expires_at_str:
            try:
                expires_at = datetime.fromisoformat(expires_at_str)
                if expires_at <= now:
                    continue
            except Exception:
                pass

        m["numBids"] += 1
        buyer_pk = bid.get("BuyerPK")
        if buyer_pk:
            bidders_by_market.setdefault(market_key, set()).add(buyer_pk)

    for market_key, bidder_set in bidders_by_market.items():
        m = markets.get(market_key)
        if m:
            m["numBidders"] = len(bidder_set)

    return jsonify({"ok": True, "items": list(markets.values())})



@bp.get("/device-market")
@require_role("buyers", "sellers", "admin")  # or just "buyers"
def get_device_market_range():
    """
    Given a DevicePK and grade (A/B/C), return the platform price range
    for that grade, plus optional release price / release date.

    This is used by BidWizardModal to show the allowed bidding band.
    """
    table = current_app.ddb_table

    device_pk = request.args.get("devicePk")
    grade = (request.args.get("grade") or "").upper()

    if not device_pk or not grade:
        return jsonify(error="devicePk and grade are required"), 400

    # Look up the device PROFILE row
    resp = table.get_item(Key={"PK": device_pk, "SK": "PROFILE"})
    item = resp.get("Item")
    if not item:
        return jsonify(error="Device not found"), 404

    def _num(v):
        if isinstance(v, Decimal):
            return float(v)
        try:
            return float(v)
        except Exception:
            return None

    if grade == "A":
        min_v = item.get("Grade_A_MIN")
        max_v = item.get("Grade_A_MAX")
    elif grade == "B":
        min_v = item.get("Grade_B_MIN")
        max_v = item.get("Grade_B_MAX")
    elif grade == "C":
        min_v = item.get("Grade_C_MIN")
        max_v = item.get("Grade_C_MAX")
    else:
        return jsonify(error="grade must be A, B, or C"), 400

    out = {
        "platformMin": _num(min_v),
        "platformMax": _num(max_v),
        "releasePrice": _num(item.get("ReleasePrice")),
        "releaseDate": item.get("ReleaseDate"),
    }

    return jsonify(out)

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
    unread = sum(1 for it in items if not bool(it.get("Read", it.get("Read", False))))

    return jsonify({"ok": True, "count": unread})



@bp.get("/notifications")
@require_role("buyers", "sellers", "admin")
def list_notifications():
    """
    Return notifications for the current user in the shape expected by the
    React NotificationsPage:
      { id, type, title, message, createdAt, isRead }
    """
    user_pk = _user_pk_from_session()
    if not user_pk:
        return ("Unauthorized", 401)

    table = current_app.ddb_table
    notif_pk = f"NOTIF#{user_pk}"

    resp = table.query(
        KeyConditionExpression=Key("PK").eq(notif_pk),
        ScanIndexForward=False,  # newest first
        Limit=int(request.args.get("limit", 100)),
    )
    items = resp.get("Items", []) or []

    def _format_rm(value):
        try:
            return f"RM {float(value):,.2f}"
        except Exception:
            return "the agreed price"

    ALLOWED_NOTIFICATION_TYPES = {
        "TRADE_MATCHED",
        "BID_EXPIRED",
        "LISTING_EXPIRED_NO_MATCH",
        "PAYMENT_COMPLETED",
    }

    def _canon_type(raw_type: str | None) -> str | None:
        if not raw_type:
            return None
        t = str(raw_type).strip().upper()
        return t if t in ALLOWED_NOTIFICATION_TYPES else None


    projected: list[dict] = []

    for raw in items:
        notif_type = _canon_type(raw.get("Type") or raw.get("type"))
        if not notif_type:
            # Skip legacy/unknown notifications so your page stays consistent
            continue

        user_role = (raw.get("UserRole") or raw.get("Role") or "buyer").lower()
        market_key = raw.get("MarketKey") or ""
        trade_price = raw.get("TradePrice")
        created_at = raw.get("CreatedAt") or raw.get("PaidAt") or raw.get("MatchedAt") or raw.get("EndedAt") or ""

        # Standardize read flag
        read_val = raw.get("Read", raw.get("read", False))
        if isinstance(read_val, bool):
            is_read = read_val
        elif isinstance(read_val, str):
            is_read = read_val.strip().lower() == "true"
        else:
            is_read = False

        message_from_item = raw.get("Message")

        # Titles + messages
        if notif_type == "TRADE_MATCHED":
            price_str = _format_rm(trade_price)
            if user_role == "buyer":
                title = "Your bid has been matched"
                message = message_from_item or (
                    f"Your bid in market {market_key} has been matched at {price_str}. "
                    "Please proceed to checkout to complete the purchase."
                )
            elif user_role == "seller":
                title = "Your listing has been matched with a buyer"
                message = message_from_item or (
                    f"Your listing in market {market_key} has been matched with a buyer at {price_str}. "
                    "Check your seller dashboard for the trade details."
                )
            else:
                title = "Trade matched"
                message = message_from_item or f"A trade in market {market_key} was matched at {price_str}."

        elif notif_type == "BID_EXPIRED":
            title = "Bid expired"
            message = message_from_item or f"Your bid in market {market_key} has expired."

        elif notif_type == "LISTING_EXPIRED_NO_MATCH":
            title = "Listing expired without match"
            message = message_from_item or f"Your listing in market {market_key} expired without any matching bid."

        elif notif_type == "PAYMENT_COMPLETED":
            price_str = _format_rm(trade_price)
            if user_role == "seller":
                title = "Payment received"
                message = message_from_item or (
                    f"The buyer has completed payment for your listing in market {market_key} at {price_str}. "
                    "You may now proceed with settlement."
                )
            else:
                title = "Payment successful"
                message = message_from_item or (
                    f"Your payment for market {market_key} has been recorded at {price_str}. "
                    "Thank you for completing your purchase."
                )

        else:
            title = "Update on your bids and listings"
            message = message_from_item or "You have a new notification in DeviceLoop."

        projected.append(
            {
                "id": raw.get("SK"),
                "type": notif_type,
                "title": title,
                "message": message,
                "createdAt": created_at,
                "isRead": is_read,
            }
        )

    unread_count = sum(1 for n in projected if not n["isRead"])
    return jsonify({"ok": True, "items": projected, "unreadCount": unread_count})



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
    from boto3.dynamodb.conditions import Attr

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

# at the top of buyer_routes.py (if not already there)
import urllib.parse

# ...

@bp.get("/listings/<path:listing_id>")
@require_role("buyers", "admin")
def get_listing_details(listing_id: str):
    """
    Return full details for a single listing for the buyer details page.
    Accepts a URL-encoded listing_id (LISTREQ%23USER%23002%2317642...)
    and decodes it back to the DynamoDB PK (LISTREQ#USER#002#1764237088).
    """
    listing_id = urllib.parse.unquote(listing_id)
    current_app.logger.info("Buyer requesting listing details for %s", listing_id)

    table = current_app.ddb_table

    # Try LISTING first, then LISTING_REQUEST (your existing patterns)
    item = None
    for key in (
        {"PK": listing_id, "SK": "LISTING"},
        {"PK": listing_id, "SK": "LISTING_REQUEST"},
    ):
        resp = table.get_item(Key=key)
        item = resp.get("Item")
        if item:
            current_app.logger.info(
                "Listing %s found in DynamoDB with key %s", listing_id, key
            )
            break

    if not item:
        current_app.logger.warning("Listing %s not found in DynamoDB", listing_id)
        return jsonify({"error": "Listing not found"}), 404

    # --- Convert raw S3 keys in Photos → presigned HTTPS URLs ---
    photos_raw = item.get("Photos") or {}
    photos_signed: dict[str, str] = {}

    if isinstance(photos_raw, dict):
        for label, key in photos_raw.items():
            if not key:
                continue
            try:
                url = presign_get(key)
                photos_signed[str(label)] = url
            except Exception as exc:
                current_app.logger.warning(
                    "Failed to presign photo %r (%s): %s", label, key, exc
                )
    else:
        photos_signed = photos_raw  # legacy shape – just pass through

    # Helper to convert Decimal → float
    def _num(v):
        if isinstance(v, Decimal):
            return float(v)
        return v

    # ---- Load device PROFILE row for release + platform range ----
    device_pk = item.get("DevicePK")
    device_profile: dict | None = None
    if device_pk:
        try:
            resp = table.get_item(Key={"PK": device_pk, "SK": "PROFILE"})
            device_profile = resp.get("Item") or None
        except Exception as exc:
            current_app.logger.warning("Failed to load device profile %s: %s", device_pk, exc)

    final_grade = item.get("FinalGrade")

    # Release metadata – prefer PROFILE, fall back to listing
    release_date = None
    release_price = None
    if device_profile:
        release_date = device_profile.get("ReleaseDate")
        release_price = device_profile.get("ReleasePrice")
    release_date = release_date or item.get("ReleaseDate") or item.get("releaseDate")
    release_price = release_price or item.get("ReleasePrice")

    # ---- Platform range (for this device / grade) ----
    platform_min = None
    platform_max = None

    if device_profile and final_grade:
        g = str(final_grade).upper()
        if g == "A":
            platform_min = device_profile.get("Grade_A_MIN")
            platform_max = device_profile.get("Grade_A_MAX")
        elif g == "B":
            platform_min = device_profile.get("Grade_B_MIN")
            platform_max = device_profile.get("Grade_B_MAX")
        elif g == "C":
            platform_min = device_profile.get("Grade_C_MIN")
            platform_max = device_profile.get("Grade_C_MAX")

    # Fallback: if we still have nothing, try any generic grade min/max on listing
    if platform_min is None and platform_max is None:
        platform_min = item.get("GradeMinPrice")
        platform_max = item.get("GradeMaxPrice")

    platform_range = None
    if platform_min is not None or platform_max is not None:
        platform_range = {
            "min": _num(platform_min),
            "max": _num(platform_max),
        }

    # ---- Device info for frontend ----
    device = {
        "brand": item.get("Brand", ""),
        "category": item.get("Category", ""),
        "model": item.get("Model", ""),
        "variant": item.get("Variant"),
        "ram": item.get("RAM"),
        "storage": item.get("Storage"),
        "grade": final_grade,
        "photos": photos_signed,
        "title": item.get("Title", ""),
        "devicePk": device_pk,
        "releaseDate": release_date,
        "releasePrice": _num(release_price) if release_price is not None else None,
    }

    # ---- Auction info ----
    auction = {
        "auctionMode": item.get("AuctionMode", "continuous"),
        "status": item.get("Status", "active"),
        "startsAt": item.get("AuctionStartsAt"),
        "endsAt": item.get("AuctionEndsAt"),
        "sellerMin": _num(item.get("SellerMin", 0)),
        "sellerMax": _num(item.get("SellerMax", 0)),
        # Kept for future use, but not displayed in UI now
        "currentTradePrice": _num(item.get("CurrentTradePrice")),
        "finalMin": _num(item.get("FinalMin")),
        "finalMax": _num(item.get("FinalMax")),
    }

    questionnaire = item.get("Questionnaire")
    market_key = item.get("MarketKey", "")

    return jsonify(
        {
            "ok": True,
            "listingId": listing_id,
            "marketKey": market_key,
            "device": device,
            "auction": auction,
            "platformRange": platform_range,
            "questionnaire": questionnaire,
        }
    )

@bp.get("/purchases")
@require_role("buyers", "admin")
def get_purchases():
    """
    Return all listings where the current user is the matched buyer.
    Used by the buyer cart page.
    """
    buyer_pk = _buyer_pk_from_session()
    if not buyer_pk:
        return ("Unauthorized", 401)

    table = current_app.ddb_table

    # All LISTING_REQUEST rows where this buyer was matched
    resp = table.scan(
        FilterExpression=Attr("SK").eq("LISTING_REQUEST")
        & Attr("MatchedBuyerPK").eq(buyer_pk)
    )
    items = resp.get("Items", [])

    def _infer_seller_pk_from_listing_pk(listing_pk: str | None) -> str | None:
        """
        listing_pk often looks like:
          LISTREQ#USER#002#17565668374
        If SellerPK isn't stored, infer seller as USER#002.
        """
        if not listing_pk or not isinstance(listing_pk, str):
            return None
        if not listing_pk.startswith("LISTREQ#"):
            return None
        rest = listing_pk[len("LISTREQ#"):]
        parts = rest.split("#")
        # USER#002#<timestamp>  -> USER#002
        if len(parts) >= 2:
            return "#".join(parts[:2])
        return parts[0] if parts else None

    def _infer_grade(market_key: str | None, fallback: str | None) -> str | None:
        if fallback:
            return fallback
        if not market_key:
            return None
        # e.g. Device#079#A  -> A
        try:
            g = str(market_key).split("#")[-1]
            return g if g else None
        except Exception:
            return None

    results: list[dict] = []
    for it in items:
        listing_pk = it.get("PK")
        market_key = it.get("MarketKey")

        matched_at = (
            it.get("MatchedAt")
            or it.get("EndedAt")
            or it.get("AuctionEndsAt")
        )

        # Prefer CurrentTradePrice (interval/end-of-window), then MatchedTradePrice (continuous), then TradePrice
        raw_trade_price = (
            it.get("CurrentTradePrice")
            or it.get("MatchedTradePrice")
            or it.get("TradePrice")
            or 0
        )
        try:
            trade_price = float(raw_trade_price)
        except (TypeError, ValueError):
            trade_price = 0.0

        payment_status = it.get("PaymentStatus", "pending")
        paid_at = it.get("PaidAt")

        seller_pk = (
            it.get("SellerPK")
            or it.get("SellerPk")
            or _infer_seller_pk_from_listing_pk(listing_pk)
        )

        auction_mode = (
            it.get("AuctionMode")
            or it.get("auctionMode")
            or it.get("Mode")
            # For older rows that didn't store it, pick a sensible default
            or "continuous"
        )

        brand = it.get("Brand") or it.get("DeviceBrand")
        model = it.get("Model") or it.get("DeviceModel")
        variant = it.get("Variant") or it.get("DeviceVariant")
        grade = _infer_grade(
            market_key,
            it.get("FinalGrade") or it.get("InitialGrade") or it.get("Grade")
        )

        results.append(
            {
                "listingId": listing_pk,
                "marketKey": market_key,
                "brand": brand,
                "model": model,
                "variant": variant,
                "grade": grade,
                "sellerPk": seller_pk,
                "auctionMode": auction_mode,
                "matchedAt": matched_at,
                "tradePrice": trade_price,
                "paymentStatus": payment_status,
                "paidAt": paid_at,
                "status": it.get("ListingStatus") or it.get("Status"),
            }
        )

    return jsonify({"ok": True, "items": results})


@bp.post("/purchases/<listing_id>/pay")
@require_role("buyers", "admin")
def pay_for_purchase(listing_id: str):
    """
    Mark a matched listing as paid by the current buyer and
    create payment notifications for BOTH buyer and seller.
    """
    buyer_pk = _buyer_pk_from_session()
    if not buyer_pk:
        return ("Unauthorized", 401)

    table = current_app.ddb_table

    listing_pk = listing_id
    listing_sk = "LISTING_REQUEST"

    now = datetime.now(timezone.utc)
    paid_at_iso = now.isoformat()

    try:
        # 1) Mark listing as paid (only if it was already matched and not paid)
        table.update_item(
            Key={"PK": listing_pk, "SK": listing_sk},
            UpdateExpression="""
                SET PaymentStatus = :paid,
                    PaidAt = :paidAt
            """,
            ExpressionAttributeValues={
                ":paid": "paid",
                ":paidAt": paid_at_iso,
            },
            ConditionExpression=Attr("MatchedBuyerPK").eq(buyer_pk)
            & (Attr("PaymentStatus").ne("paid") | Attr("PaymentStatus").not_exists()),
        )

        # 2) Reload listing to include all metadata for the response + notifications
        listing_resp = table.get_item(Key={"PK": listing_pk, "SK": listing_sk})
        listing = listing_resp.get("Item")
        if not listing:
            current_app.logger.error(
                "Listing not found after payment update: %s", listing_pk
            )
            return jsonify({"ok": False, "error": "Listing not found"}), 404

        # Compute trade price again (for notifications)
        raw_trade_price = (
            listing.get("CurrentTradePrice")
            or listing.get("MatchedTradePrice")
            or listing.get("TradePrice")
            or 0
        )
        try:
            trade_price_num = float(raw_trade_price)
        except (TypeError, ValueError):
            trade_price_num = 0.0

        # 3) Create payment notifications for buyer + seller
        market_key = listing.get("MarketKey")
        brand = listing.get("Brand")
        model = listing.get("Model")
        variant = listing.get("Variant")
        seller_pk = listing.get("SellerPK")
        listing_status = listing.get("ListingStatus") or listing.get("Status") or "ended"

        # Store price in Dynamo as Decimal
        trade_price_d = Decimal(str(trade_price_num)) if trade_price_num else Decimal("0")

        # Common base for notifications
        def make_notif(pk: str, role: str, suffix: str) -> dict:
            return {
                "PK": f"NOTIF#{pk}",
                "SK": f"TS#{paid_at_iso}#PAYMENT#{suffix}",
                "Type": "PAYMENT_COMPLETED",
                "UserPK": pk,
                "UserRole": role,
                "MarketKey": market_key,
                "Brand": brand,
                "Model": model,
                "Variant": variant,
                "TradePrice": trade_price_d,
                "ListingPK": listing_pk,
                "ListingStatus": listing_status,
                "PaymentStatus": "paid",
                "PaidAt": paid_at_iso,
                "CreatedAt": paid_at_iso,
                "Read": False,
            }

        # Buyer notification
        buyer_notif = make_notif(buyer_pk, "buyer", "BUYER")
        table.put_item(Item=buyer_notif)

        try:
            buyer_email = _ddb_get_user_email(table, buyer_pk)  # must return string or None
            if buyer_email:
                subject = "DeviceLoop: Payment completed"
                device_name = " ".join([str(x) for x in [brand, model, variant] if x])
                body = (
                    f"Your payment has been completed.\n\n"
                    f"Listing: {listing_pk}\n"
                    f"Market: {market_key}\n"
                    f"Device: {device_name}\n"
                    f"Trade price: {trade_price_num:.2f}\n"
                    f"Paid at: {paid_at_iso}\n"
                )
                _send_email(buyer_email, subject, body)
                current_app.logger.info(
                    "[EMAIL] PAYMENT_COMPLETED buyer sent",
                    extra={"to": buyer_email, "listingPk": listing_pk, "marketKey": market_key},
                )
            else:
                current_app.logger.warning(
                    "[EMAIL] PAYMENT_COMPLETED buyer missing email",
                    extra={"buyerPk": buyer_pk, "listingPk": listing_pk},
                )
        except Exception:
            # Do NOT fail payment if email fails
            current_app.logger.exception(
                "[EMAIL] PAYMENT_COMPLETED buyer send failed",
                extra={"buyerPk": buyer_pk, "listingPk": listing_pk},
            )


        # Seller notification (if we know the seller)
        if seller_pk:
            seller_notif = make_notif(seller_pk, "seller", "SELLER")
            table.put_item(Item=seller_notif)

            try:
                seller_email = _ddb_get_user_email(table, seller_pk)
                if seller_email:
                    subject = "DeviceLoop: Buyer payment completed"
                    device_name = " ".join([str(x) for x in [brand, model, variant] if x])
                    body = (
                        f"The buyer has completed payment.\n\n"
                        f"Listing: {listing_pk}\n"
                        f"Market: {market_key}\n"
                        f"Device: {device_name}\n"
                        f"Trade price: {trade_price_num:.2f}\n"
                        f"Paid at: {paid_at_iso}\n"
                    )
                    _send_email(seller_email, subject, body)
                    current_app.logger.info(
                        "[EMAIL] PAYMENT_COMPLETED seller sent",
                        extra={"to": seller_email, "listingPk": listing_pk, "marketKey": market_key},
                    )
                else:
                    current_app.logger.warning(
                        "[EMAIL] PAYMENT_COMPLETED seller missing email",
                        extra={"sellerPk": seller_pk, "listingPk": listing_pk},
                    )
            except Exception:
                current_app.logger.exception(
                    "[EMAIL] PAYMENT_COMPLETED seller send failed",
                    extra={"sellerPk": seller_pk, "listingPk": listing_pk},
                )

        # 4) Response for the frontend
        return jsonify(
            {
                "ok": True,
                "listingId": listing_pk,
                "paidAt": paid_at_iso,
            }
        )

    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            current_app.logger.error(
                "Failed conditional payment update for listing %s / buyer %s",
                listing_pk,
                buyer_pk,
            )
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": "cannot mark as paid",
                    }
                ),
                400,
            )

        current_app.logger.exception("Failed to mark purchase as paid")
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "failed to mark as paid",
                }
            ),
            500,
        )

@bp.post("/notifications/mark-read")
@require_role("buyers", "sellers", "admin")
def mark_notifications_read():
    """
    Mark notifications as read.
    If ids is missing or empty, mark the latest batch as read (same behavior as mark-all-read).
    """
    user_pk = _user_pk_from_session()
    if not user_pk:
        return ("Unauthorized", 401)

    payload = request.get_json(silent=True) or {}
    ids = payload.get("ids") or []

    table = current_app.ddb_table
    notif_pk = f"NOTIF#{user_pk}"
    now_iso = _utc_now_iso()

    # If no ids provided: reuse your existing logic (mark latest 50 as read)
    if not ids:
        resp = table.query(
            KeyConditionExpression=Key("PK").eq(notif_pk),
            Limit=50,
            ScanIndexForward=False,
        )
        items = resp.get("Items", [])
        for it in items:
            if it.get("Read"):
                continue
            table.update_item(
                Key={"PK": it["PK"], "SK": it["SK"]},
                UpdateExpression="SET #read = :true, ReadAt = :now",
                ExpressionAttributeNames={"#read": "Read"},
                ExpressionAttributeValues={":true": True, ":now": now_iso},
            )
        return jsonify({"ok": True, "count": len(items)})

    # If ids provided: ids are the SK values from the notification list
    updated = 0
    for sk in ids:
        try:
            table.update_item(
                Key={"PK": notif_pk, "SK": sk},
                UpdateExpression="SET #read = :true, ReadAt = :now",
                ExpressionAttributeNames={"#read": "Read"},
                ExpressionAttributeValues={":true": True, ":now": now_iso},
            )
            updated += 1
        except Exception:
            current_app.logger.exception("Failed to mark notification read")
    return jsonify({"ok": True, "count": updated})
