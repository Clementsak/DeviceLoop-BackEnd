from flask import Blueprint, jsonify, session, current_app, request
from .auth_routes import _find_user_pk_by_sub, _profile_key
from datetime import datetime, timezone
import boto3
from .auth_routes import _find_user_pk_by_sub, _profile_key
from decimal import Decimal

bp = Blueprint("views", __name__)


def _iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _location_client():
    region = current_app.config.get("AWS_REGION", "ap-southeast-1")
    return boto3.client("location", region_name=region)


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

    profile_role = "buyers"
    profile_groups = []
    verified = False

    if pk:
        r = table.get_item(Key=_profile_key(pk), ConsistentRead=True).get("Item", {}) or {}
        profile_role   = r.get("Role", "buyers")
        profile_groups = r.get("Groups") or []
        verified       = bool(r.get("IsVerified", False))

    cognito_groups = u.get("cognito:groups") or u.get("groups") or []
    if not isinstance(cognito_groups, list):
        cognito_groups = []

    all_groups = sorted({
        str(g).lower()
        for g in [*cognito_groups, *profile_groups]
        if isinstance(g, str) and g
    })

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
        "groups": all_groups,
        "verified": bool(verified),                 # <--- ONLY your DDB flag
        "email_verified": bool(u.get("email_verified", False)),
        "phone_number_verified": bool(u.get("phone_number_verified", False)),
    })


    return jsonify(user={
        "sub": sub,
        "email": u.get("email"),
        "phone_number": u.get("phone_number"),
        "role": effective_role,
        "groups": all_groups,                       # ALWAYS an array
        "verified": verified or bool(u.get("email_verified", False)),
    })

@bp.post("/api/verify/buyer")
def request_buyer_verification():
    """
    Body:
    {
      "termsAccepted": true,
      "coords": { "lat": number, "lon": number, "accuracy"?: number }
    }
    """
    u = session.get("user")
    if not u:
        return jsonify(error="Not logged in"), 401

    table = current_app.ddb_table
    sub = u["sub"]
    user_pk = _find_user_pk_by_sub(table, sub)
    if not user_pk:
        return jsonify(error="User profile not found"), 404

    body = request.get_json(force=True) or {}
    terms_accepted = bool(body.get("termsAccepted"))
    if not terms_accepted:
        return jsonify(error="termsAccepted is required"), 400

    coords = body.get("coords") or {}
    lat = coords.get("lat")
    lon = coords.get("lon")
    accuracy = coords.get("accuracy")
    if not isinstance(coords, dict) or coords.get("lat") is None or coords.get("lon") is None:
        return jsonify(error="coords (lat/lon) is required"), 400

    # reverse geocode via Amazon Location Service
    place = None
    if lat is not None and lon is not None:
        try:
            index_name = current_app.config.get(
                "AWS_LOCATION_INDEX", "deviceloop-place-index"
            )
            loc = _location_client()
            resp = loc.search_place_index_for_position(
                IndexName=index_name,
                Position=[float(lon), float(lat)],  # AWS expects [lon, lat]
                MaxResults=1,
            )
            results = resp.get("Results") or []
            if results:
                raw_place = results[0].get("Place", {})
                # Only keep simple string fields � no floats, no Geometry
                place = {
                    "label": raw_place.get("Label"),
                    "country": raw_place.get("Country"),
                    "region": raw_place.get("Region"),
                    "city": raw_place.get("Municipality"),
                    "postalCode": raw_place.get("PostalCode"),
                }
        except Exception as e:
            current_app.logger.warning("Location lookup failed: %s", e)

    now = _iso_now()


    coords_map: dict[str, object] = {}
    if lat is not None:
        coords_map["lat"] = Decimal(str(lat))
    if lon is not None:
        coords_map["lon"] = Decimal(str(lon))
    if accuracy is not None:
        coords_map["accuracy"] = Decimal(str(accuracy))

    data = {
        "termsAccepted": True,
        "termsAcceptedAt": now,
        "coords": coords_map,
        "ip": request.headers.get("X-Forwarded-For", request.remote_addr),
        "cognitoEmail": u.get("email"),
        "cognitoPhone": u.get("phone_number"),
        "cognitoAddress": u.get("address"),
    }
    if place is not None:
        data["place"] = place  # strings only

    item = {
        "PK": user_pk,
        "SK": "VERIFY#USER#ACTIVE",
        "Type": "VerifyUser",
        "Status": "pending",
        "SubmittedAt": now,
        "GSI2PK": "VERIFY#PENDING#user",
        "GSI2SK": now,
        "Data": data,
    }

    table.put_item(Item=item)

    table.update_item(
        Key=_profile_key(user_pk),
        UpdateExpression="SET VerifiedStatus = :vs",
        ExpressionAttributeValues={":vs": "pending"},
    )

    return jsonify(ok=True)

@bp.post("/api/verify/seller")
def request_seller_registration():
    """
    Body:
    {
      "organisationName": string,
      "organisationRegNo"?: string,
      "address"?: string,
      "contactEmail"?: string,
      "contactPhone"?: string,
      "website"?: string,
      "notes"?: string
    }
    """
    u = session.get("user")
    if not u:
        return jsonify(error="Not logged in"), 401

    table = current_app.ddb_table
    sub = u["sub"]
    user_pk = _find_user_pk_by_sub(table, sub)
    if not user_pk:
        return jsonify(error="User profile not found"), 404

    body = request.get_json(force=True) or {}
    org_name = (body.get("organisationName") or "").strip()
    if not org_name:
        return jsonify(error="organisationName is required"), 400

    now = _iso_now()

    seller_profile = {
        "organisationName": org_name,
        "organisationRegNo": (body.get("organisationRegNo") or "").strip(),
        "address": (body.get("address") or u.get("address") or "").strip(),
        "contactEmail": (body.get("contactEmail") or u.get("email") or "").strip(),
        "contactPhone": (body.get("contactPhone") or u.get("phone_number") or "").strip(),
        "website": (body.get("website") or "").strip(),
        "notes": (body.get("notes") or "").strip(),
        "submittedAt": now,
    }

    item = {
        "PK": user_pk,
        "SK": "VERIFY#SELLER#ACTIVE",
        "Type": "VerifySeller",
        "Status": "pending",
        "SubmittedAt": now,
        "GSI2PK": "VERIFY#PENDING#seller",
        "GSI2SK": now,
        "SellerProfile": seller_profile,
    }

    table.put_item(Item=item)

    # You can optionally set another field on the profile to show "sellerPending"
    # but your admin verify queue already distinguishes kind=user|seller.

    return jsonify(ok=True)
