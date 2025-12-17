# auth_routes.py
import os
from urllib.parse import quote
from flask import Blueprint, current_app, redirect, session, url_for
from datetime import datetime, timezone
import botocore
import boto3
from boto3.dynamodb.types import TypeSerializer


bp = Blueprint("auth", __name__)

DEFAULT_GROUP = os.getenv("DEFAULT_USER_ROLE", "buyers")  # <- make sure this matches the *group name*
GROUP_ORDER = ["admin", "sellers", "buyers"]
client = boto3.client("dynamodb", region_name=os.getenv("AWS_REGION", "ap-southeast-1"))
ser = TypeSerializer()


def _cfg(name: str, default: str | None = None) -> str:
    v = current_app.config.get(name, default)
    if v is None:
        raise RuntimeError(f"Missing config: {name}")
    return v

def _idp():
    region = current_app.config.get("AWS_REGION", "ap-southeast-1")
    return boto3.client("cognito-idp", region_name=region)

def _user_pool_id() -> str:
    upid = current_app.config.get("COGNITO_USERPOOL_ID")
    if not upid:
        raise RuntimeError("COGNITO_USERPOOL_ID is not configured")
    return upid

# Utility functions for user profile management
def _iso_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def role_from_groups(groups: list[str]) -> str:
    for g in GROUP_ORDER:
        if g in groups:
            return g
    return "buyers"

def _marshal(d: dict) -> dict:
    return {k: ser.serialize(v) for k, v in d.items()}

#Keys for DynamoDB items
def _sub_map_key(sub: str):
    # This item allows us to find the numeric id from Cognito sub.
    return {"PK": f"SUB#{sub}", "SK": "MAP"}

def _profile_key(user_pk: str):
    return {"PK": user_pk, "SK": "PROFILE"}

def _next_user_seq(table):
    """
    Increments and returns a global user sequence number.
    Uses a single counter item: { PK: 'COUNTER#USER', SK: 'SEQ', Value: N }
    """
    resp = table.update_item(
        Key={"PK": "COUNTER#USER", "SK": "SEQ"},
        UpdateExpression="SET #v = if_not_exists(#v, :zero) + :inc",
        ExpressionAttributeNames={"#v": "Value"},
        ExpressionAttributeValues={":zero": 0, ":inc": 1},
        ReturnValues="UPDATED_NEW",
    )
    return int(resp["Attributes"]["Value"])

def _user_pk_from_num(n: int) -> str:
    return f"USER#{n:03d}"


# Cognito helpers
def _cognito_username_from_sub(sub: str) -> str | None:
    r = _idp().list_users(UserPoolId=_user_pool_id(), Filter=f'sub = "{sub}"', Limit=1)
    users = r.get("Users", [])
    return users[0]["Username"] if users else None

def _groups_for_sub(sub: str) -> list[str]:
    u = _cognito_username_from_sub(sub)
    if not u:
        return []
    resp = _idp().admin_list_groups_for_user(UserPoolId=_user_pool_id(), Username=u)
    return [g["GroupName"] for g in resp.get("Groups", [])]

def _ensure_group_membership(sub: str, group: str = DEFAULT_GROUP) -> None:
    username = _cognito_username_from_sub(sub)
    if not username:
        return
    groups = _idp().admin_list_groups_for_user(UserPoolId=_user_pool_id(), Username=username).get("Groups", [])
    if any(g["GroupName"] == group for g in groups):
        return
    try:
        _idp().admin_add_user_to_group(UserPoolId=_user_pool_id(), Username=username, GroupName=group)
    except botocore.exceptions.ClientError as e:
        # harmless if group doesn't exist or already added; log if you like
        pass

def _get_user_attribute_by_sub(sub: str, attr_name: str) -> str | None:
    username = _cognito_username_from_sub(sub)
    if not username:
        return None

    resp = _idp().admin_get_user(UserPoolId=_user_pool_id(), Username=username)
    attrs = resp.get("UserAttributes", []) or []
    for a in attrs:
        if a.get("Name") == attr_name:
            return a.get("Value")
    return None

#DynamoDB user profile sync
def _find_user_pk_by_sub(table, sub: str) -> str | None:
    r = table.get_item(Key=_sub_map_key(sub), ConsistentRead=True)
    return r.get("Item", {}).get("UserPK")

def _create_first_time_user(table, sub: str, email: str | None, phone: str | None):
    n = _next_user_seq(table)
    user_pk = _user_pk_from_num(n)

    profile = {
        **_profile_key(user_pk),
        "Type": "User",
        "Status": "active",
        "Sub": sub,
        "Email": (email or "").lower(),
        "PhoneNo": (phone or ""),
        "CreatedAt": _iso_now(),
        "LastLoginAt": _iso_now(),
        "IsVerified": False,
        "VerifiedStatus": "pending",
        "GSI1PK": "ROLE#buyers",
        "GSI1SK": user_pk,
        "GSI3PK": f"EMAIL#{(email or '').lower()}",
        "GSI3SK": user_pk,
    }
    submap = {**_sub_map_key(sub), "UserPK": user_pk}

    try:
        client.transact_write_items(
            TransactItems=[
                {
                    "Put": {
                        "TableName": table.name,
                        "Item": _marshal(profile),
                        "ConditionExpression": "attribute_not_exists(PK) AND attribute_not_exists(SK)",
                    }
                },
                {
                    "Put": {
                        "TableName": table.name,
                        "Item": _marshal(submap),
                        "ConditionExpression": "attribute_not_exists(PK) AND attribute_not_exists(SK)",
                    }
                },
            ]
        )
        return user_pk, True  # created
    except client.exceptions.TransactionCanceledException as e:
        # If someone else created concurrently (or this user already existed), fall back to lookup.
        existing = _find_user_pk_by_sub(table, sub)
        if existing:
            return existing, False
        raise

def _touch_last_login(table, user_pk: str):
    table.update_item(
        Key=_profile_key(user_pk),
        UpdateExpression="SET LastLoginAt = :ts",
        ExpressionAttributeValues={":ts": _iso_now()},
        )

def _sync_role_from_groups(table, user_pk: str, sub: str):
    groups = _groups_for_sub(sub)
    new_role = role_from_groups(groups)
    # Only update if changed
    table.update_item(
        Key=_profile_key(user_pk),
        UpdateExpression="SET #role = :r, Groups = :g, GSI1PK=:pk, GSI1SK=:sk",
        ExpressionAttributeNames={"#role": "Role"},
        ExpressionAttributeValues={":r": new_role, ":g": groups, ":pk": f"ROLE#{new_role}", ":sk": user_pk,},
    )

#Route handlers
@bp.get("/login")
def login():
    redirect_uri = url_for("auth.callback", _external=True, _scheme="https")
    return current_app.oauth.oidc.authorize_redirect(redirect_uri, prompt="login")

@bp.get("/signup")
def signup():
    redirect_uri = url_for("auth.callback", _external=True, _scheme="https")
    return current_app.oauth.oidc.authorize_redirect(redirect_uri, screen_hint="signup")

@bp.get("/callback")
def callback():
    token = current_app.oauth.oidc.authorize_access_token()
    user  = token.get("userinfo") or {}

    sub   = user.get("sub")
    email = user.get("email")
    phone = user.get("phone_number")
    # Try standard attribute first; fall back to a custom attribute if you used one
    addr_str = _get_user_attribute_by_sub(sub, "address") or _get_user_attribute_by_sub(sub, "custom:address")

    session["user"] = {
        "sub": sub,
        "email": email,
        "phone_number": phone,
        "address": addr_str,
    }

    table = current_app.ddb_table
    user_pk = _find_user_pk_by_sub(table, sub)
    if user_pk:
        _touch_last_login(table, user_pk)
    else:
        user_pk, _ = _create_first_time_user(table, sub, email, phone, )

    session["userPk"] = user_pk

    _ensure_group_membership(sub, DEFAULT_GROUP)
    _sync_role_from_groups(table, user_pk, sub)

    return redirect(_cfg("FRONTEND_AFTER_LOGIN", "https://localhost:5173/"))

@bp.get("/logout")
@bp.post("/logout")
def logout():
    """Clear Flask session and redirect through Cognito logout flow."""
    session.clear()
    HOSTED = _cfg("COGNITO_HOSTED_DOMAIN")  # domain only, no protocol
    CLIENT_ID = _cfg("COGNITO_CLIENT_ID")
    SIGNOUT_CALLBACK = _cfg("COGNITO_SIGNOUT_CALLBACK", "https://localhost:5000/auth/signout-callback")

    # Step 1: Hosted UI logout
    logout_url = (
        f"https://{HOSTED}/logout"
        f"?client_id={CLIENT_ID}"
        f"&logout_uri={quote(SIGNOUT_CALLBACK, safe='')}"
    )
    return redirect(logout_url)

@bp.get("/signout-callback")
def signout_callback():
    """Final hop after Cognito logout — back to frontend."""
    return redirect(_cfg("FRONTEND_AFTER_LOGIN", "https://localhost:5173/"))


