# guards.py (or inside auth_routes.py)
from functools import wraps
from flask import session, current_app
from .auth_routes import _find_user_pk_by_sub, _profile_key

def require_role(*allowed):
    def deco(fn):
        @wraps(fn)
        def inner(*a, **kw):
            u = session.get("user")
            if not u:
                return ("Unauthorized", 401)
            table = current_app.ddb_table
            pk = _find_user_pk_by_sub(table, u["sub"])
            if not pk:
                return ("Forbidden", 403)
            item = table.get_item(Key=_profile_key(pk), ConsistentRead=True).get("Item", {})
            role = item.get("Role", "buyers")
            if role not in allowed:
                return ("Forbidden", 403)
            return fn(*a, **kw)
        return inner
    return deco
