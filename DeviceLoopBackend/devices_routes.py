# DeviceLoopBackend/DeviceLoopBackend/devices_routes.py
import base64, csv, io, json
from datetime import datetime, timezone
from typing import Any
from decimal import Decimal, InvalidOperation


from flask import Blueprint, current_app, jsonify, request, send_file
from boto3.dynamodb.conditions import Key, Attr

# optional dependency for .xlsx read/write
try:
    import openpyxl
    from openpyxl.workbook import Workbook  # imported when available
except Exception:
    openpyxl = None
    Workbook = None  # type: ignore

from .guards import require_role

bp = Blueprint("devices", __name__, url_prefix="/admin/devices")

# ---------- helpers ----------
_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSY  = {"0", "false", "no", "n", "off", "f"}
CANON_MAP = {
    "storage": "Storage",
    "ram": "RAM",
    "releaseDate": "ReleaseDate",
    "releasePrice": "ReleasePrice",
}

PRICE_COLS = [
    "Grade_A_MAX","Grade_A_MIN",
    "Grade_B_MAX","Grade_B_MIN",
    "Grade_C_MAX","Grade_C_MIN",
]

def _pick_price_cols(it: dict) -> dict:
    out = {}
    for k in PRICE_COLS:
        v = it.get(k)
        if isinstance(v, Decimal): v = float(v)
        out[k] = v
    return out

def _iso_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _encode_cursor(key: dict | None) -> str | None:
    if not key: return None
    return base64.urlsafe_b64encode(json.dumps(key).encode()).decode()

def _decode_cursor(s: str | None) -> dict | None:
    if not s: return None
    return json.loads(base64.urlsafe_b64decode(s.encode()).decode())

def _gsi1() -> str:
    # IMPORTANT: this must point to your Device index (Category->Brand/Model)
    # set DDB_GSI1="GSI1" in config if your table uses that name.
    return current_app.config.get("DDB_GSI1", "GSI1")

def _table():
    return current_app.ddb_table

def _device_pk(n: int) -> str:
    return f"Device#{n:03d}"

def _to_decimal_or_none(v):
    if v is None: return None
    s = str(v).strip().replace(",", "")
    if not s: return None
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None

def _concat_device(it: dict) -> str:
    parts = [
        str(it.get("Brand","")).strip(),
        str(it.get("Model","")).strip(),
        str(it.get("Variant","") or "").strip(),
        str(it.get("Storage","") or "").strip(),
        str(it.get("RAM","") or "").strip(),
    ]
    # remove empties and join with spaces, collapse repeated spaces
    label = " ".join([p for p in parts if p])
    return " ".join(label.split())

def _next_device_seq(table) -> int:
    r = table.update_item(
        Key={"PK": "COUNTER#DEVICE", "SK": "SEQ"},
        UpdateExpression="SET #v = if_not_exists(#v, :zero) + :inc",
        ExpressionAttributeNames={"#v": "Value"},
        ExpressionAttributeValues={":zero": 0, ":inc": 1},
        ReturnValues="UPDATED_NEW",
    )
    return int(r["Attributes"]["Value"])

def _device_key(pk: str) -> dict[str, str]:
    return {"PK": pk, "SK": "PROFILE"}

def _price_fields_out(it: dict) -> dict:
    out = {}
    for k, v in it.items():
        if isinstance(k, str) and k.startswith("Price"):
            out[k] = float(v) if isinstance(v, Decimal) else v
    return out

def _project_device(it: dict[str, Any]) -> dict[str, Any]:
    def _num_out(x):
        return float(x) if isinstance(x, Decimal) else x

    return {
        "pk": it["PK"],
        "category": it.get("Category"),
        "brand": it.get("Brand"),
        "model": it.get("Model"),
        "variant": it.get("Variant"),
        "active": bool(it.get("Active", True)),
        "isModified": bool(it.get("isModified", False)),
        "updatedAt": it.get("UpdatedAt"),
        # NEW fields (optional)
        "storage": it.get("Storage"),
        "ram": it.get("RAM"),
        "releaseDate": it.get("ReleaseDate"),
        "releasePrice": _num_out( it.get("ReleasePrice")),
    }

def _gsi1_values_for(category: str, brand: str, model: str, pk: str) -> tuple[str, str]:
    return f"CAT#{category}", f"BRAND#{brand}#MODEL#{model}#{pk}"

def _to_bool(v: Any, default: bool | None = None) -> bool | None:
    if v is None: return default
    if isinstance(v, bool): return v
    s = str(v).strip().lower()
    if s in _TRUTHY: return True
    if s in _FALSY:  return False
    return default

def _norm_str(v: Any) -> str:
    return "" if v is None else str(v).strip()

def _want_row_is_modified(v: Any) -> bool:
    return _to_bool(v, False) is True

def _read_rows_from_upload(f) -> list[dict[str, Any]]:
    """
    Accept .csv or .xlsx, return rows as list of dicts using the header row.
    Expected headers (case-insensitive):
      required: PK, Category, Brand, Model, Variant, Active, isModified
      optional: Storage, RAM, Release Date, Release Price
    """
    name = (f.filename or "").lower()
    data = f.read()
    rows: list[dict[str, Any]] = []

    if name.endswith(".csv"):
        text = data.decode("utf-8-sig", errors="replace")
        rdr = csv.DictReader(io.StringIO(text))
        for r in rdr:
            rows.append({(k or "").strip(): (v.strip() if isinstance(v, str) else v) for k, v in r.items()})
        return rows

    if name.endswith(".xlsx"):
        if openpyxl is None:
            raise ValueError("openpyxl is not installed. Install openpyxl>=3.1")
        wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
        ws = wb.worksheets[0]
        it = ws.iter_rows(values_only=True)
        headers = [str(h).strip() if h is not None else "" for h in next(it)]
        for r in it:
            row = {headers[i]: (str(r[i]).strip() if r[i] is not None else "") for i in range(len(headers))}
            rows.append(row)
        return rows

    raise ValueError("Unsupported file type. Upload .csv or .xlsx")

def _extract_fields(row: dict[str, Any]) -> dict[str, Any]:
    def pick(*names: str) -> Any:
        for n in names:
            if n in row: return row[n]
            # case-insensitive lookup
            for k in row.keys():
                if k.lower() == n.lower():
                    return row[k]
        return None
    def _num(v):
        if v is None:
            return None
        s = str(v).strip()
        if not s:
            return None
        s = s.replace(",", "")            # allow "3,899"
        try:
            return Decimal(s)             # <- return Decimal, not float
        except (InvalidOperation, ValueError):
            return None

    def _date(v):
        s = str(v).strip() if v is not None else ""
        if not s: return None
        return s  # keep normalized as text

    pk        = _norm_str(pick("PK"))
    category  = _norm_str(pick("Category"))
    brand     = _norm_str(pick("Brand"))
    model     = _norm_str(pick("Model"))
    variant   = _norm_str(pick("Variant")) or None
    active    = _to_bool(pick("Active"), True)
    is_mod    = _to_bool(pick("isModified"), False)
    # NEW optional inputs
    storage   = _norm_str(pick("Storage")) or None
    ram       = _norm_str(pick("RAM")) or None
    rel_date  = _date(pick("Release Date"))
    rel_price = _num(pick("Release Price"))

    return {
        "pk": pk, "category": category, "brand": brand, "model": model,
        "variant": variant, "active": active, "isModified": is_mod,
        "storage": storage, "ram": ram, "releaseDate": rel_date, "releasePrice": rel_price,
    }

# ---------- list/query ----------
@bp.get("")
@require_role("admin")
def list_devices():
    table = _table()
    category = (request.args.get("category") or "All").strip()
    brand    = (request.args.get("brand") or "").strip()
    q        = (request.args.get("q") or "").strip().lower()
    active_s = (request.args.get("active") or "all").strip().lower()
    limit    = max(1, min(int(request.args.get("limit", "25")), 100))
    cursor   = request.args.get("cursor")

    # base filter for non-deleted device profiles
    base_fe = (
        Attr("SK").eq("PROFILE")
        & Attr("Type").eq("Device")
        & (Attr("Status").not_exists() | Attr("Status").ne("deleted"))
    )

    def matches(it: dict) -> bool:
        # active filter
        if active_s in ("true", "false"):
            want = (active_s == "true")
            if bool(it.get("Active", True)) != want:
                return False
        # text search
        if q:
            b = str(it.get("Brand", "")).lower()
            m = str(it.get("Model", "")).lower()
            v = str(it.get("Variant", "")).lower()
            if q not in b and q not in m and q not in v:
                return False
        return True

    # Gather up to `limit` *after* applying the filters, pulling more pages if needed
    collected: list[dict] = []
    lek = _decode_cursor(cursor)
    # internal page size – larger than or equal to requested page size to reduce roundtrips
    chunk = max(100, limit)

    while len(collected) < limit:
        if category and category != "All":
            gpk = f"CAT#{category}"
            key_expr = Key("GSI1PK").eq(gpk)
            if brand:
                key_expr = key_expr & Key("GSI1SK").begins_with(f"BRAND#{brand}")
            qargs = {"IndexName": _gsi1(), "KeyConditionExpression": key_expr, "Limit": chunk}
            if lek:
                qargs["ExclusiveStartKey"] = lek
            resp = table.query(**qargs)
        else:
            sargs = {"FilterExpression": base_fe, "Limit": chunk}
            if lek:
                sargs["ExclusiveStartKey"] = lek
            resp = table.scan(**sargs)

        items = resp.get("Items", [])
        lek   = resp.get("LastEvaluatedKey")

        # When Category=All and user typed a Brand, post-filter for brand here.
        if category == "All" and brand:
            bl = brand.lower()
            items = [it for it in items if bl in str(it.get("Brand", "")).lower()]

        # keep only valid device profiles and apply search/active filter
        for it in items:
            if it.get("SK") != "PROFILE" or it.get("Type") != "Device" or it.get("Status") == "deleted":
                continue
            if matches(it):
                collected.append(it)
                if len(collected) >= limit:
                    break

        if not lek:  # table exhausted
            break

    # Only expose a next cursor if we actually stopped early due to page size
    next_cursor = _encode_cursor(lek) if lek and len(collected) >= limit else None
    out = [_project_device(it) for it in collected]
    return jsonify(items=out, cursor=next_cursor)


@bp.get("/brands")
@require_role("admin")
def list_brands():
    """Return unique brand names for the given category (dropdown)."""
    category = (request.args.get("category") or "").strip()
    if not category:
        return jsonify(items=[])
    table = _table()
    gpk = f"CAT#{category}"
    brands: set[str] = set()

    kwargs = {
        "IndexName": _gsi1(),
        "KeyConditionExpression": Key("GSI1PK").eq(gpk),
        "ProjectionExpression": "#b, SK, PK, #t, #s",
        "ExpressionAttributeNames": {"#b": "Brand", "#t": "Type", "#s": "Status"},
    }
    resp = table.query(**kwargs)
    def add(resp):
        for it in resp.get("Items", []):
            if it.get("Type") == "Device" and it.get("Status") != "deleted":
                b = str(it.get("Brand") or "").strip()
                if b: brands.add(b)
    add(resp)
    while resp.get("LastEvaluatedKey"):
        resp = table.query(**kwargs, ExclusiveStartKey=resp["LastEvaluatedKey"])
        add(resp)

    return jsonify(items=sorted(brands))

# ---------- CRUD ----------

@bp.post("")
@require_role("admin")
def create_device():
    body = request.get_json(silent=True) or {}
    category = (body.get("category") or "").strip()
    brand    = (body.get("brand") or "").strip()
    model    = (body.get("model") or "").strip()
    variant  = (body.get("variant") or "").strip() or None
    active   = bool(body.get("active", True))
    is_mod   = bool(body.get("isModified", False))

    if not category or not brand or not model:
        return jsonify(error="category, brand, and model are required"), 400

    table = _table()
    pk = _device_pk(_next_device_seq(table))
    gpk, gsk = _gsi1_values_for(category, brand, model, pk)

    item = {
        "PK": pk, "SK": "PROFILE", "Type": "Device",
        "Category": category, "Brand": brand, "Model": model,
        "Variant": variant,
        "Active": active, "isModified": is_mod,
        "CreatedAt": _iso_now(), "UpdatedAt": _iso_now(),
        "GSI1PK": gpk, "GSI1SK": gsk,
    }
    # pass-through extra fields (Storage/RAM/etc.)
    for k, v in body.items():
        if k in {"category","brand","model","variant","active","isModified"}: continue
        if k in item: continue
        target = CANON_MAP.get(k, k)  # map storage->Storage etc.
        if target in item:  # avoid overwriting existing keys
            continue
        if target == "ReleasePrice":
            dv = _to_decimal_or_none(v)
            if dv is not None:
                item[target] = dv
        else:
            item[target] = v

    table.put_item(Item=item, ConditionExpression="attribute_not_exists(PK)")
    return jsonify(ok=True, device=_project_device(item))

@bp.patch("/<pk>")
@require_role("admin")
def update_device(pk: str):
    body = request.get_json(silent=True) or {}
    table = _table()

    r = table.get_item(Key=_device_key(pk), ConsistentRead=True)
    cur = r.get("Item")
    if not cur or cur.get("Type") != "Device":
        return jsonify(error="Device not found"), 404

    new_category = (body.get("category", cur.get("Category")) or "").strip()
    new_brand    = (body.get("brand", cur.get("Brand")) or "").strip()
    new_model    = (body.get("model", cur.get("Model")) or "").strip()
    new_variant  = (body.get("variant", cur.get("Variant")) or None)
    new_active   = bool(body.get("active", cur.get("Active", True)))
    new_is_mod   = bool(body.get("isModified", cur.get("isModified", False)))

    expr = ["UpdatedAt=:ts", "Category=:cat", "Brand=:br", "Model=:md", "Variant=:vr", "Active=:ac", "isModified=:im"]
    names: dict[str,str] = {}
    vals  = {
        ":ts": _iso_now(),
        ":cat": new_category, ":br": new_brand, ":md": new_model, ":vr": new_variant,
        ":ac": new_active, ":im": new_is_mod,
    }

    if (new_category, new_brand, new_model) != (cur.get("Category"), cur.get("Brand"), cur.get("Model")):
        gpk, gsk = _gsi1_values_for(new_category, new_brand, new_model, pk)
        expr += ["GSI1PK=:g1pk", "GSI1SK=:g1sk"]
        vals[":g1pk"] = gpk
        vals[":g1sk"] = gsk

    # pass-through any other fields (Storage/RAM/etc.)
    for k, v in body.items():
        if k in {"category","brand","model","variant","active","isModified"}: continue
        if k in {"PK","SK","Type","GSI1PK","GSI1SK","Status"}: continue
        target = CANON_MAP.get(k, k)
        val = _to_decimal_or_none(v) if target == "ReleasePrice" else v
        names[f"#f_{target}"] = target
        expr.append(f"#f_{target} = :v_{target}")
        vals[f":v_{target}"] = val

    kwargs = {
        "Key": _device_key(pk),
        "UpdateExpression": "SET " + ", ".join(expr),
        "ExpressionAttributeValues": vals,
    }
    if names:
        kwargs["ExpressionAttributeNames"] = names
    table.update_item(**kwargs)

    r2 = table.get_item(Key=_device_key(pk), ConsistentRead=True)
    return jsonify(ok=True, device=_project_device(r2["Item"]))

@bp.post("/<pk>/active")
@require_role("admin")
def toggle_active(pk: str):
    body = request.get_json(silent=True) or {}
    active = bool(body.get("active", True))
    table = _table()
    table.update_item(
        Key=_device_key(pk),
        UpdateExpression="SET Active=:a, UpdatedAt=:ts",
        ExpressionAttributeValues={":a": active, ":ts": _iso_now()},
    )
    return jsonify(ok=True, pk=pk, active=active)

@bp.delete("/<pk>")
@require_role("admin")
def delete_device(pk: str):
    hard = (request.args.get("hard") == "true")
    table = _table()
    if hard:
        table.delete_item(Key=_device_key(pk))
        return jsonify(ok=True, pk=pk, hard=True)

    table.update_item(
        Key=_device_key(pk),
        UpdateExpression="SET #s=:d, UpdatedAt=:ts REMOVE GSI1PK, GSI1SK",
        ExpressionAttributeNames={"#s": "Status"},
        ExpressionAttributeValues={":d": "deleted", ":ts": _iso_now()},
    )
    return jsonify(ok=True, pk=pk, hard=False)

# ---------- Import (bulk upsert) ----------

@bp.post("/import")
@require_role("admin")
def import_devices_upsert():
    if "file" not in request.files:
        return jsonify(error="Upload a file in field 'file' (.csv or .xlsx)"), 400

    file = request.files["file"]
    try:
        rows = _read_rows_from_upload(file)
    except Exception as e:
        return jsonify(error=f"Failed to read file: {e}"), 400

    table = _table()
    created = updated = skipped = 0
    errors: list[dict[str, Any]] = []
    preview: list[dict[str, Any]] = []

    for idx, raw in enumerate(rows, start=2):  # header = row 1
        try:
            f = _extract_fields(raw)
            if not _want_row_is_modified(f["isModified"]):
                skipped += 1
                continue

            pk, category, brand, model, variant, active = (
                f["pk"], f["category"], f["brand"], f["model"], f["variant"], bool(f["active"])
            )
            if not category or not brand or not model:
                raise ValueError("Missing Category/Brand/Model")
            
            if not pk:
                # create
                pk = _device_pk(_next_device_seq(table))
                gpk, gsk = _gsi1_values_for(category, brand, model, pk)
                item = {
                    "PK": pk, "SK": "PROFILE", "Type": "Device",
                    "Category": category, "Brand": brand, "Model": model,
                    "Variant": variant,
                    "Active": active, "isModified": False,
                    "CreatedAt": _iso_now(), "UpdatedAt": _iso_now(),
                    "GSI1PK": gpk, "GSI1SK": gsk,
                }
                # NEW optional fields
                if f.get("storage") is not None:      item["Storage"] = f["storage"]
                if f.get("ram") is not None:          item["RAM"] = f["ram"]
                if f.get("releaseDate") is not None:  item["ReleaseDate"] = f["releaseDate"]
                if f.get("releasePrice") is not None: item["ReleasePrice"] = f["releasePrice"]

                table.put_item(Item=item, ConditionExpression="attribute_not_exists(PK)")
                created += 1
                preview.append({"action": "create", "pk": pk, "category": category, "brand": brand, "model": model})
            else:
                # update
                r = table.get_item(Key=_device_key(pk), ConsistentRead=True)
                cur = r.get("Item")
                if not cur or cur.get("Type") != "Device":
                    raise ValueError(f"Device {pk} not found")

                names: dict[str, str] = {}
                vals: dict[str, Any] = {":ts": _iso_now()}
                set_parts: list[str] = ["UpdatedAt = :ts"]
                remove_parts: list[str] = []

                def set_field(attr: str, val):
                    alias = f"#n_{attr}"
                    names[alias] = attr
                    # blank/None means remove the attribute
                    if val is None or (isinstance(val, str) and val.strip() == ""):
                        remove_parts.append(alias)
                    else:
                        set_parts.append(f"{alias} = :v_{attr}")
                        vals[f":v_{attr}"] = val

                # core fields from the row
                set_field("Category",   category)
                set_field("Brand",      brand)
                set_field("Model",      model)
                set_field("Variant",    variant)
                set_field("Active",     active)
                set_field("isModified", False)

                # optional fields from the parsed row 'f'
                if "storage" in f:       set_field("Storage", f["storage"])
                if "ram" in f:           set_field("RAM", f["ram"])
                if "releaseDate" in f:   set_field("ReleaseDate", f["releaseDate"])
                if "releasePrice" in f:  set_field("ReleasePrice", f["releasePrice"])

                # if CBM changes, also update GSI keys (aliased)
                if (category, brand, model) != (
                    cur.get("Category"), cur.get("Brand"), cur.get("Model")
                ):
                    gpk, gsk = _gsi1_values_for(category, brand, model, pk)
                    names["#g1pk"] = "GSI1PK"
                    names["#g1sk"] = "GSI1SK"
                    set_parts.append("#g1pk = :g1pk")
                    set_parts.append("#g1sk = :g1sk")
                    vals[":g1pk"] = gpk
                    vals[":g1sk"] = gsk

                update_expr = "SET " + ", ".join(set_parts)
                if remove_parts:
                    update_expr += " REMOVE " + ", ".join(remove_parts)

                table.update_item(
                    Key={"PK": pk, "SK": "PROFILE"},
                    UpdateExpression=update_expr,
                    ExpressionAttributeNames=names,
                    ExpressionAttributeValues=vals,
                    ConditionExpression=Attr("PK").exists(),
                )

                updated += 1
                preview.append({"action": "update", "pk": pk, "category": category, "brand": brand, "model": model})
        except Exception as e:
            errors.append({"row": idx, "reason": str(e)})

    return jsonify(ok=True, created=created, updated=updated, skipped=skipped, errors=errors, preview=preview[:50])

# ---------- Exports (XLSX) ----------

def _fetch_filtered_devices_for_export(table, category: str, brand: str, q: str, active_s: str) -> list[dict]:
    """fetch all rows (ignore pagination) honoring filters; used by both export endpoints."""
    base_fe = (
        Attr("SK").eq("PROFILE")
        & Attr("Type").eq("Device")
        & (Attr("Status").not_exists() | Attr("Status").ne("deleted"))
    )

    items: list[dict] = []

    if category and category != "All":
        gpk = f"CAT#{category}"
        kwargs = {"IndexName": _gsi1(), "KeyConditionExpression": Key("GSI1PK").eq(gpk)}
        if brand:
            kwargs["KeyConditionExpression"] = Key("GSI1PK").eq(gpk) & Key("GSI1SK").begins_with(f"BRAND#{brand}")
        resp = table.query(**kwargs)
        items.extend(resp.get("Items", []))
        while resp.get("LastEvaluatedKey"):
            resp = table.query(**kwargs, ExclusiveStartKey=resp["LastEvaluatedKey"])
            items.extend(resp.get("Items", []))
        items = [it for it in items if it.get("SK") == "PROFILE" and it.get("Type") == "Device" and it.get("Status") != "deleted"]
    else:
        resp = table.scan(FilterExpression=base_fe)
        items.extend(resp.get("Items", []))
        while resp.get("LastEvaluatedKey"):
            resp = table.scan(FilterExpression=base_fe, ExclusiveStartKey=resp["LastEvaluatedKey"])
            items.extend(resp.get("Items", []))
        if brand:
            bl = brand.lower()
            items = [it for it in items if bl in str(it.get("Brand","")).lower()]

    if q:
        ql = q.lower()
        items = [it for it in items if ql in str(it.get("Brand","")).lower()
                 or ql in str(it.get("Model","")).lower()
                 or ql in str(it.get("Variant","")).lower()]

    if active_s in ("true", "false"):
        want = (active_s == "true")
        items = [it for it in items if bool(it.get("Active", True)) == want]

    return items

def _xlsx_response(wb: Any, filename: str):
    mem = io.BytesIO()
    wb.save(mem)
    mem.seek(0)
    return send_file(
        mem,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

@bp.get("/export/table")
@require_role("admin")
def export_table_xlsx():
    """
    Single-sheet XLSX of exactly the visible table (honors filters).
    """
    if openpyxl is None:
        return jsonify(error="openpyxl is required on server for XLSX export"), 500

    table = _table()
    category = (request.args.get("category") or "All").strip()
    brand    = (request.args.get("brand") or "").strip()
    q        = (request.args.get("q") or "").strip()
    active_s = (request.args.get("active") or "all").strip().lower()

    items = _fetch_filtered_devices_for_export(table, category, brand, q, active_s)

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Devices"
    ws.append([
        "PK", "Category", "Brand", "Model", "Variant",
        "Storage", "RAM", "ReleaseDate", "ReleasePrice",
        "Active", "isModified", "UpdatedAt"
    ])

    for r in items:
        price = r.get("ReleasePrice")
        price_out = float(price) if isinstance(price, Decimal) else (price or "")

        ws.append([
            r.get("PK"),
            r.get("Category"),
            r.get("Brand"),
            r.get("Model"),
            r.get("Variant") or "",
            r.get("Storage") or "",
            r.get("RAM") or "",
            r.get("ReleaseDate") or "",
            price_out,
            "TRUE" if r.get("Active", True) else "FALSE",
            "TRUE" if r.get("isModified", False) else "FALSE",
            r.get("UpdatedAt") or "",
        ])

    return _xlsx_response(wb, "devices_table.xlsx")

@bp.get("/export/grouped")
@require_role("admin")
def export_grouped_xlsx():
    """
    Three sheets: Laptop, Smartphone, Tablet (unified columns for consistency).
    """
    if openpyxl is None:
        return jsonify(error="openpyxl is required on server for XLSX export"), 500

    table = _table()
    category = (request.args.get("category") or "All").strip()
    brand    = (request.args.get("brand") or "").strip()
    q        = (request.args.get("q") or "").strip()
    active_s = (request.args.get("active") or "all").strip().lower()

    items = _fetch_filtered_devices_for_export(table, category, brand, q, active_s)

    buckets = {"Laptop": [], "Smartphone": [], "Tablet": []}
    for it in items:
        buckets.get(it.get("Category"), []).append(it)

    wb = openpyxl.Workbook()
    wb.remove(wb.active)

    def fill_sheet(name: str, rows: list[dict]):
        ws = wb.create_sheet(name)
        ws.append(["PK", "Brand", "Model", "Variant", "Storage", "RAM", "Active", "isModified", "UpdatedAt"])
        for r in rows:
            ws.append([
                r.get("PK"),
                r.get("Brand"),
                r.get("Model"),
                r.get("Variant") or "",
                r.get("Storage") or "",
                r.get("RAM") or "",
                "TRUE" if r.get("Active", True) else "FALSE",
                "TRUE" if r.get("isModified", False) else "FALSE",
                r.get("UpdatedAt") or "",
            ])

    fill_sheet("Laptop",     buckets["Laptop"])
    fill_sheet("Smartphone", buckets["Smartphone"])
    fill_sheet("Tablet",     buckets["Tablet"])

    return _xlsx_response(wb, "devices_grouped.xlsx")

@bp.get("/prices")
@require_role("admin")
def list_prices():
    table = _table()
    category = (request.args.get("category") or "All").strip()
    brand    = (request.args.get("brand") or "").strip()
    q        = (request.args.get("q") or "").strip().lower()
    active_s = (request.args.get("active") or "all").strip().lower()
    limit    = max(1, min(int(request.args.get("limit", "25")), 100))
    cursor   = request.args.get("cursor")

    base_fe = (
        Attr("SK").eq("PROFILE")
        & Attr("Type").eq("Device")
        & (Attr("Status").not_exists() | Attr("Status").ne("deleted"))
    )

    def matches(it: dict) -> bool:
        if active_s in ("true", "false"):
            want = (active_s == "true")
            if bool(it.get("Active", True)) != want:
                return False
        if q:
            b = str(it.get("Brand", "")).lower()
            m = str(it.get("Model", "")).lower()
            v = str(it.get("Variant", "")).lower()
            if q not in b and q not in m and q not in v:
                return False
        return True

    items_out, lek = [], _decode_cursor(cursor)
    chunk = max(100, limit)

    while len(items_out) < limit:
        if category and category != "All":
            key_expr = Key("GSI1PK").eq(f"CAT#{category}")
            if brand:
                key_expr = key_expr & Key("GSI1SK").begins_with(f"BRAND#{brand}")
            qargs = {"IndexName": _gsi1(), "KeyConditionExpression": key_expr, "Limit": chunk}
            if lek: qargs["ExclusiveStartKey"] = lek
            resp = table.query(**qargs)
        else:
            sargs = {"FilterExpression": base_fe, "Limit": chunk}
            if lek: sargs["ExclusiveStartKey"] = lek
            resp = table.scan(**sargs)
            if brand:
                bl = brand.lower()
                resp["Items"] = [it for it in resp.get("Items", []) if bl in str(it.get("Brand", "")).lower()]

        items = resp.get("Items", [])
        lek   = resp.get("LastEvaluatedKey")

        for it in items:
            if not matches(it): continue
            row = _project_device(it)
            row.update(_pick_price_cols(it))
            items_out.append(row)
            if len(items_out) >= limit:
                break

        if not lek:
            break

    next_cursor = _encode_cursor(lek) if lek and len(items_out) >= limit else None
    return jsonify(items=items_out, cursor=next_cursor)


@bp.patch("/prices/<pk>")
@require_role("admin")
def admin_update_prices(pk: str):
    body = request.get_json(force=True, silent=True) or {}
    # only accept our six columns
    payload = {k: _to_decimal_or_none(body.get(k)) for k in PRICE_COLS if k in body}

    if not payload:
        return jsonify(ok=True)

    names, vals, set_parts, remove_parts = {}, {":ts": _iso_now()}, ["UpdatedAt = :ts"], []

    def set_or_remove(attr: str, dv):
        alias = f"#n_{attr}"
        names[alias] = attr
        if dv is None:
            remove_parts.append(alias)
        else:
            set_parts.append(f"{alias} = :v_{attr}")
            vals[f":v_{attr}"] = dv

    for k, dv in payload.items():
        set_or_remove(k, dv)

    update_expr = "SET " + ", ".join(set_parts)
    if remove_parts:
        update_expr += " REMOVE " + ", ".join(remove_parts)

    _table().update_item(
        Key={"PK": pk, "SK": "PROFILE"},
        UpdateExpression=update_expr,
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=vals,
        ConditionExpression=Attr("PK").exists(),
    )
    return jsonify(ok=True)



@bp.get("/prices/export/table")
@require_role("admin")
def admin_export_prices_table():
    if openpyxl is None:
        return jsonify(error="openpyxl is required on server for XLSX export"), 500

    table = _table()
    category = (request.args.get("category") or "All").strip()
    brand    = (request.args.get("brand") or "").strip()
    q        = (request.args.get("q") or "").strip()
    active_s = (request.args.get("active") or "all").strip().lower()

    items = _fetch_filtered_devices_for_export(table, category, brand, q, active_s)

    wb = openpyxl.Workbook()
    ws = wb.active; ws.title = "Prices"

    headers = ["PK","Device","Active","isModified","UpdatedAt"] + PRICE_COLS
    ws.append(headers)

    for it in items:
        row = [
            it.get("PK"),
            _concat_device(it),
            "TRUE" if it.get("Active", True) else "FALSE",
            "TRUE" if it.get("isModified", False) else "FALSE",
            it.get("UpdatedAt") or "",
        ]
        for k in PRICE_COLS:
            v = it.get(k)
            if isinstance(v, Decimal): v = float(v)
            row.append(v or "")
        ws.append(row)

    return _xlsx_response(wb, "prices_table.xlsx")

@bp.post("/prices/import")
@require_role("admin")
def admin_import_prices():
    file = request.files.get("file")
    if not file:
        return jsonify(error="file is required"), 400

    rows = _read_rows_from_upload(file)
    updated = 0; skipped = 0; errors = []

    for i, r in enumerate(rows, start=2):
        try:
            pk = (r.get("PK") or "").strip()
            mod = str(r.get("isModified") or "").strip().lower() in ("true","1","yes","y")
            if not mod: 
                skipped += 1; 
                continue
            if not pk:
                raise ValueError("Missing PK")

            payload = {}
            for k in PRICE_COLS:
                if k in r:
                    payload[k] = _to_decimal_or_none(r.get(k))

            if not payload:
                skipped += 1; 
                continue

            names, vals, set_parts, remove_parts = {}, {":ts": _iso_now()}, ["UpdatedAt = :ts"], []
            def set_or_remove(attr: str, dv):
                alias = f"#n_{attr}"
                names[alias] = attr
                if dv is None:
                    remove_parts.append(alias)
                else:
                    set_parts.append(f"{alias} = :v_{attr}")
                    vals[f":v_{attr}"] = dv

            for k, dv in payload.items():
                set_or_remove(k, dv)

            ue = "SET " + ", ".join(set_parts)
            if remove_parts: ue += " REMOVE " + ", ".join(remove_parts)

            _table().update_item(
                Key={"PK": pk, "SK": "PROFILE"},
                UpdateExpression=ue,
                ExpressionAttributeNames=names,
                ExpressionAttributeValues=vals,
                ConditionExpression=Attr("PK").exists(),
            )
            updated += 1

        except Exception as e:
            errors.append({"row": i, "reason": str(e)})

    return jsonify(ok=True, created=0, updated=updated, skipped=skipped, errors=errors)

@bp.get("/options")
@require_role("sellers", "admin")
def device_options():
    """
    Return all PROFILE rows (active devices) that have platform pricing
    so the seller can choose from dropdowns.

    Response:
    {
      "items": [
        {
          "pk": "Device#088",
          "category": "Tablet",
          "brand": "Samsung",
          "model": "Galaxy Tab S9 (5G)",
          "variant": "null or string",
          "storage": "256GB",
          "ram": "8GB",
          "gradeAmin": 4400,
          "gradeAmax": 4100,
          "gradeBmin": 3800,
          "gradeBmax": 3500,
          "gradeCmin": 3200,
          "gradeCmax": 2900
        },
        ...
      ]
    }
    """
    table = _table()

    fe = (
        Attr("SK").eq("PROFILE")
        & Attr("Active").eq(True)
        & (
            Attr("Status").not_exists()
            | Attr("Status").ne("deleted")
        )
    )

    items: list[dict] = []
    resp = table.scan(FilterExpression=fe)
    items.extend(resp.get("Items", []))

    while "LastEvaluatedKey" in resp:
        resp = table.scan(
            FilterExpression=fe,
            ExclusiveStartKey=resp["LastEvaluatedKey"],
        )
        items.extend(resp.get("Items", []))

    def _num(v):
        if isinstance(v, Decimal):
            return float(v)
        try:
            return float(v)
        except Exception:
            return None

    out: list[dict] = []
    for it in items:
        out.append(
            {
                "pk": it["PK"],                         # e.g. "Device#088"
                "category": it.get("Category"),
                "brand": it.get("Brand"),
                "model": it.get("Model"),               # exact Dynamo name
                "variant": it.get("Variant"),           # may be null
                "storage": it.get("Storage"),
                "ram": it.get("RAM"),
                "gradeAmin": _num(it.get("Grade_A_MIN")),
                "gradeAmax": _num(it.get("Grade_A_MAX")),
                "gradeBmin": _num(it.get("Grade_B_MIN")),
                "gradeBmax": _num(it.get("Grade_B_MAX")),
                "gradeCmin": _num(it.get("Grade_C_MIN")),
                "gradeCmax": _num(it.get("Grade_C_MAX")),
            }
        )

    return jsonify({"items": out})
