# devices_routes.py
import base64, csv, io, json, zipfile
from datetime import datetime, timezone
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from flask import Blueprint, current_app, jsonify, request, send_file

# Reuse your guard
from .guards import require_role
from .auth_routes import _profile_key  # for shape parity if needed

bp = Blueprint("devices", __name__, url_prefix="/admin/devices")

try:
    import openpyxl  # for .xlsx
except Exception:
    openpyxl = None

# ---- helpers ---------------------------------------------------------------
_TRUTHY = {"1","true","yes","y","on","t"}
_FALSY  = {"0","false","no","n","off","f"}

def _iso_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _encode_cursor(key: dict | None) -> str | None:
    if not key: return None
    return base64.urlsafe_b64encode(json.dumps(key).encode()).decode()

def _decode_cursor(s: str | None) -> dict | None:
    if not s: return None
    return json.loads(base64.urlsafe_b64decode(s.encode()).decode())

def _gsi1() -> str:
    # same env keys you already use elsewhere
    return current_app.config.get("DDB_GSI1", "GSI1")

def _table():
    return current_app.ddb_table

def _device_pk(n: int) -> str:
    return f"Device#{n:03d}"

def _next_device_seq(table) -> int:
    r = table.update_item(
        Key={"PK":"COUNTER#DEVICE","SK":"SEQ"},
        UpdateExpression="SET #v = if_not_exists(#v, :zero) + :inc",
        ExpressionAttributeNames={"#v":"Value"},
        ExpressionAttributeValues={":zero": 0, ":inc": 1},
        ReturnValues="UPDATED_NEW",
    )
    return int(r["Attributes"]["Value"])

def _device_key(pk: str) -> dict[str,str]:
    return {"PK": pk, "SK": "PROFILE"}

def _project_device(it: dict[str, Any]) -> dict[str, Any]:
    """Shape API payload for a device row."""
    return {
        "pk": it["PK"],
        "category": it.get("Category"),
        "brand": it.get("Brand"),
        "model": it.get("Model"),
        "variant": it.get("Variant"),
        "active": bool(it.get("Active", True)),
        "isModified": bool(it.get("isModified", False)),
        "updatedAt": it.get("UpdatedAt"),
        # include all other attributes if you need in the future
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
    # Treat blank as False; only explicit Yes/True/1 means "process"
    return _to_bool(v, False) is True

def _read_rows_from_upload(f) -> list[dict[str, Any]]:
    """
    Accepts CSV or XLSX. Returns a list of dict rows using first row as headers.
    Header names (case-insensitive) we expect at minimum:
      PK, Category, Brand, Model, Variant, Active, isModified
    """
    name = (f.filename or "").lower()
    data = f.read()
    rows: list[dict[str, Any]] = []

    if name.endswith(".csv"):
        text = data.decode("utf-8-sig", errors="replace")
        rdr = csv.DictReader(io.StringIO(text))
        for r in rdr:
            rows.append({(k or "").strip(): (v.strip() if isinstance(v, str) else v) for k, v in r.items()})

    elif name.endswith(".xlsx"):
        if openpyxl is None:
            raise ValueError("openpyxl is not installed; cannot read .xlsx. Install openpyxl>=3.1")
        wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
        # Use first sheet only; if you prefer, iterate all sheets
        ws = wb.worksheets[0]
        it = ws.iter_rows(values_only=True)
        headers = [str(h).strip() if h is not None else "" for h in next(it)]
        for r in it:
            row = {headers[i]: (str(r[i]).strip() if r[i] is not None else "") for i in range(len(headers))}
            rows.append(row)
    else:
        raise ValueError("Unsupported file type. Please upload .csv or .xlsx")

    return rows

def _extract_fields(row: dict[str, Any]) -> dict[str, Any]:
    """Map row values to our canonical fields with normalization."""
    # Be lenient with header capitalization
    def pick(*names: str) -> Any:
        for n in names:
            if n in row: return row[n]
            if n.lower() in row: return row[n.lower()]
            # case-insensitive lookup
            for k in row.keys():
                if k.lower() == n.lower():
                    return row[k]
        return None

    pk        = _norm_str(pick("PK"))
    category  = _norm_str(pick("Category"))
    brand     = _norm_str(pick("Brand"))
    model     = _norm_str(pick("Model"))
    variant   = _norm_str(pick("Variant")) or None
    active    = _to_bool(pick("Active"), True)
    is_mod    = _to_bool(pick("isModified"), False)
    # allow optional extra attributes in your sheet
    return {
        "pk": pk, "category": category, "brand": brand, "model": model, "variant": variant,
        "active": active, "isModified": is_mod
    }

# ---- list/query ------------------------------------------------------------

@bp.get("")
@require_role("admin")
def list_devices():
    """
    GET /admin/devices
      ?category = All|Laptop|Smartphone|Tablet (default All)
      ?brand    = <brand> (optional)
      ?q        = <text search on brand/model/variant>
      ?active   = all|true|false (default all)
      ?limit    = 25
      ?cursor   = <opaque>
    """
    table   = _table()
    category = (request.args.get("category") or "All").strip()
    brand    = (request.args.get("brand") or "").strip()
    q        = (request.args.get("q") or "").strip().lower()
    active_s = (request.args.get("active") or "all").strip().lower()
    limit    = max(1, min(int(request.args.get("limit", "25")), 100))
    cursor   = request.args.get("cursor")

    items: list[dict]
    next_cursor: str | None = None

    # base filter to ignore soft-deleted
    base_fe = (Attr("SK").eq("PROFILE") & Attr("Type").eq("Device") & (Attr("Status").not_exists() | Attr("Status").ne("deleted")))

    if category and category != "All":
        # Query GSI1 by category, optionally refine brand via begins_with on GSI1SK
        gpk = f"CAT#{category}"
        kwargs: dict[str, Any] = {
            "IndexName": _gsi1(),
            "KeyConditionExpression": Key("GSI1PK").eq(gpk),
            "Limit": limit,
        }
        # NOTE: DynamoDB can't do arbitrary contains on key; we can prefix-filter Brand via begins_with
        if brand:
            kwargs["KeyConditionExpression"] = Key("GSI1PK").eq(gpk) & Key("GSI1SK").begins_with(f"BRAND#{brand}")
        lek = _decode_cursor(cursor)
        if lek: kwargs["ExclusiveStartKey"] = lek
        resp = table.query(**kwargs)
        raw = resp.get("Items", [])
        next_cursor = _encode_cursor(resp.get("LastEvaluatedKey"))
        # Base filter + optional text/active filter at app layer
        items = [it for it in raw if it.get("SK") == "PROFILE" and it.get("Type") == "Device" and it.get("Status") != "deleted"]
    else:
        # Admin “All” => safe SCAN (OK at your current scale). Later you can add an ALL#DEVICE index.
        kwargs = {"FilterExpression": base_fe, "Limit": limit}
        lek = _decode_cursor(cursor)
        if lek: kwargs["ExclusiveStartKey"] = lek
        resp = table.scan(**kwargs)
        items = resp.get("Items", [])
        next_cursor = _encode_cursor(resp.get("LastEvaluatedKey"))

    # post-filters
    if q:
        ql = q.lower()
        items = [
            it for it in items
            if (str(it.get("Brand","")).lower().find(ql) >= 0)
            or (str(it.get("Model","")).lower().find(ql) >= 0)
            or (str(it.get("Variant","")).lower().find(ql) >= 0)
        ]
    if active_s in ("true","false"):
        want = (active_s == "true")
        items = [it for it in items if bool(it.get("Active", True)) == want]

    # shape
    out = [_project_device(it) for it in items]
    return jsonify(items=out, cursor=next_cursor)

# ---- create/update/toggle/delete ------------------------------------------

@bp.post("")
@require_role("admin")
def create_device():
    """
    POST /admin/devices
    Body: { category, brand, model, variant?, active?, isModified? , ...extraAttrs }
    """
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
    n  = _next_device_seq(table)
    pk = _device_pk(n)

    gpk, gsk = _gsi1_values_for(category, brand, model, pk)

    item = {
        "PK": pk, "SK": "PROFILE", "Type": "Device",
        "Category": category, "Brand": brand, "Model": model,
        "Variant": variant,
        "Active": active, "isModified": is_mod,
        "CreatedAt": _iso_now(), "UpdatedAt": _iso_now(),
        "GSI1PK": gpk, "GSI1SK": gsk,
    }
    # include any extra user-provided fields (safe merge)
    for k,v in body.items():
        if k in {"category","brand","model","variant","active","isModified"}: continue
        # avoid overwriting keys like PK/SK/Type/GSI1PK...
        if k in item: continue
        item[k] = v

    table.put_item(Item=item, ConditionExpression="attribute_not_exists(PK)")
    return jsonify(ok=True, device=_project_device(item))

@bp.patch("/<pk>")
@require_role("admin")
def update_device(pk: str):
    """
    PATCH /admin/devices/<pk>
    Body may include any of: category, brand, model, variant, active, isModified, ... (extra fields allowed)
    Will recompute GSI1 if category/brand/model changed.
    """
    body = request.get_json(silent=True) or {}
    table = _table()

    # Load current
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
    names = {}
    vals  = {
        ":ts": _iso_now(),
        ":cat": new_category, ":br": new_brand, ":md": new_model, ":vr": new_variant,
        ":ac": new_active, ":im": new_is_mod,
    }

    # Rewrite GSI1 values if any of (category/brand/model) changed
    if (new_category, new_brand, new_model) != (cur.get("Category"), cur.get("Brand"), cur.get("Model")):
        gpk, gsk = _gsi1_values_for(new_category, new_brand, new_model, pk)
        expr += ["GSI1PK=:g1pk", "GSI1SK=:g1sk"]
        vals[":g1pk"] = gpk
        vals[":g1sk"] = gsk

    # Extra unknown fields
    for k,v in body.items():
        if k in {"category","brand","model","variant","active","isModified"}: continue
        # avoid core keys
        if k in {"PK","SK","Type","GSI1PK","GSI1SK","Status"}: continue
        names[f"#f_{k}"] = k
        expr.append(f"#f_{k} = :v_{k}")
        vals[f":v_{k}"] = v

    update_expr = "SET " + ", ".join(expr)

    kwargs = {
        "Key": _device_key(pk),
        "UpdateExpression": update_expr,
        "ExpressionAttributeValues": vals,
    }
    if names:                         # only include when non-empty
        kwargs["ExpressionAttributeNames"] = names

    table.update_item(**kwargs)

    # Return fresh
    r2 = table.get_item(Key=_device_key(pk), ConsistentRead=True)
    return jsonify(ok=True, device=_project_device(r2["Item"]))

@bp.post("/<pk>/active")
@require_role("admin")
def toggle_active(pk: str):
    """
    POST /admin/devices/<pk>/active
    Body: { active: true|false }
    """
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
    """
    DELETE /admin/devices/<pk>?hard=true
    Soft delete by default (Status='deleted' and remove GSI1 attrs).
    """
    hard = (request.args.get("hard") == "true")
    table = _table()
    if hard:
        table.delete_item(Key=_device_key(pk))
        return jsonify(ok=True, pk=pk, hard=True)

    table.update_item(
        Key=_device_key(pk),
        UpdateExpression="SET #s=:d, UpdatedAt=:ts REMOVE GSI1PK, GSI1SK",
        ExpressionAttributeNames={"#s":"Status"},
        ExpressionAttributeValues={":d":"deleted", ":ts": _iso_now()},
    )
    return jsonify(ok=True, pk=pk, hard=False)

@bp.post("/import")
@require_role("admin")
def import_devices_upsert():
    """
    Multipart upload: field name 'file' (.csv or .xlsx).
    Only rows where isModified == Yes/True/1 are processed.
    If PK is empty -> create; else update.
    Flips isModified to False after success.

    Returns: { created, updated, skipped, errors: [{row, reason}], preview?: [...] }
    """
    if "file" not in request.files:
        return jsonify(error="Upload a file in field 'file' (.csv or .xlsx)"), 400

    file = request.files["file"]
    try:
        rows = _read_rows_from_upload(file)
    except Exception as e:
        return jsonify(error=f"Failed to read file: {e}"), 400

    table = _table()

    created = 0
    updated = 0
    skipped = 0
    errors  = []

    # Optional: collect a short preview of what we applied
    preview: list[dict[str, Any]] = []

    for idx, raw in enumerate(rows, start=2):  # 1 = header, so data starts at 2 for Excel-like row numbers
        try:
            fields = _extract_fields(raw)
            if not _want_row_is_modified(fields["isModified"]):
                skipped += 1
                continue

            pk        = fields["pk"]
            category  = fields["category"]
            brand     = fields["brand"]
            model     = fields["model"]
            variant   = fields["variant"]
            active    = bool(fields["active"])

            # minimal validation
            if not category or not brand or not model:
                raise ValueError("Missing Category/Brand/Model")

            if not pk:
                # --- CREATE ---
                n  = _next_device_seq(table)
                pk = _device_pk(n)
                gpk, gsk = _gsi1_values_for(category, brand, model, pk)
                item = {
                    "PK": pk, "SK": "PROFILE", "Type": "Device",
                    "Category": category, "Brand": brand, "Model": model,
                    "Variant": variant,
                    "Active": active, "isModified": False,   # flip to False
                    "CreatedAt": _iso_now(), "UpdatedAt": _iso_now(),
                    "GSI1PK": gpk, "GSI1SK": gsk,
                }
                table.put_item(Item=item, ConditionExpression="attribute_not_exists(PK)")
                created += 1
                preview.append({"action":"create", "pk": pk, "category": category, "brand": brand, "model": model})
            else:
                # --- UPDATE ---
                # Load existing item
                r = table.get_item(Key=_device_key(pk), ConsistentRead=True)
                cur = r.get("Item")
                if not cur or cur.get("Type") != "Device":
                    raise ValueError(f"Device {pk} not found")

                exprs = ["UpdatedAt=:ts", "Category=:cat", "Brand=:br", "Model=:md", "Variant=:vr", "Active=:ac", "isModified=:im"]
                vals  = {
                    ":ts": _iso_now(), ":cat": category, ":br": brand, ":md": model,
                    ":vr": variant, ":ac": active, ":im": False  # flip to False
                }
                # recompute GSI if identity fields changed
                if (category, brand, model) != (cur.get("Category"), cur.get("Brand"), cur.get("Model")):
                    gpk, gsk = _gsi1_values_for(category, brand, model, pk)
                    exprs += ["GSI1PK=:g1pk", "GSI1SK=:g1sk"]
                    vals[":g1pk"] = gpk
                    vals[":g1sk"] = gsk

                table.update_item(
                    Key=_device_key(pk),
                    UpdateExpression="SET " + ", ".join(exprs),
                    ExpressionAttributeValues=vals,
                )
                updated += 1
                preview.append({"action":"update", "pk": pk, "category": category, "brand": brand, "model": model})

        except Exception as e:
            errors.append({"row": idx, "reason": str(e)})
            continue

    return jsonify(
        ok=True,
        created=created,
        updated=updated,
        skipped=skipped,
        errors=errors,
        preview=preview[:50]  # cap preview
    )

# devices_routes.py (add)
@bp.get("/brands")
@require_role("admin")
def list_brands():
    """GET /admin/devices/brands?category=Laptop -> {items: ["Apple","Dell",...]}"""
    category = (request.args.get("category") or "").strip()
    if category not in {"Laptop","Smartphone","Tablet"}:
        return jsonify(error="category must be Laptop|Smartphone|Tablet"), 400

    table = _table()
    gpk = f"CAT#{category}"

    brands = set()
    lek = None
    while True:
        resp = table.query(
            IndexName=_gsi1(),
            KeyConditionExpression=Key("GSI1PK").eq(gpk),
            ProjectionExpression="Brand, SK, #t, #s",
            ExpressionAttributeNames={"#t":"Type","#s":"Status"},
            ExclusiveStartKey=lek or None,
        )
        for it in resp.get("Items", []):
            if it.get("SK")=="PROFILE" and it.get("Type")=="Device" and it.get("Status")!="deleted":
                b = (it.get("Brand") or "").strip()
                if b: brands.add(b)
        lek = resp.get("LastEvaluatedKey")
        if not lek: break

    return jsonify(items=sorted(brands))

# --- XLSX helpers -----------------------------------------------------------
def _xlsx_from_items(items: list[dict], grouped: bool):
    if openpyxl is None:
        raise RuntimeError("openpyxl not installed")
    from openpyxl import Workbook

    wb = Workbook()
    wb.remove(wb.active)

    def add_sheet(name: str, rows: list[dict]):
        ws = wb.create_sheet(title=name)
        headers = ["PK","Category","Brand","Model","Variant","Active","isModified","UpdatedAt"]
        ws.append(headers)
        for r in rows:
            ws.append([
                r.get("PK"),
                r.get("Category"),
                r.get("Brand"),
                r.get("Model"),
                r.get("Variant") or "",
                "TRUE" if bool(r.get("Active", True)) else "FALSE",
                "TRUE" if bool(r.get("isModified", False)) else "FALSE",
                r.get("UpdatedAt") or "",
            ])

    if grouped:
        add_sheet("Laptop",     [r for r in items if r.get("Category")=="Laptop"])
        add_sheet("Smartphone", [r for r in items if r.get("Category")=="Smartphone"])
        add_sheet("Tablet",     [r for r in items if r.get("Category")=="Tablet"])
    else:
        add_sheet("Devices", items)

    bio = io.BytesIO()
    wb.save(bio); bio.seek(0)
    return bio

def _fetch_filtered_devices_for_export():
    """Use the same filters as list_devices; returns a full (unpaged) list."""
    table    = _table()
    category = (request.args.get("category") or "All").strip()
    brand    = (request.args.get("brand") or "").strip()
    q        = (request.args.get("q") or "").strip().lower()
    active_s = (request.args.get("active") or "all").strip().lower()

    fe = Attr("SK").eq("PROFILE") & Attr("Type").eq("Device") & (Attr("Status").not_exists() | Attr("Status").ne("deleted"))
    items: list[dict] = []

    if category and category != "All":
        gpk = f"CAT#{category}"
        lek = None
        while True:
            kwargs = {
                "IndexName": _gsi1(),
                "KeyConditionExpression": Key("GSI1PK").eq(gpk)
            }
            if brand:
                kwargs["KeyConditionExpression"] = Key("GSI1PK").eq(gpk) & Key("GSI1SK").begins_with(f"BRAND#{brand}")
            if lek: kwargs["ExclusiveStartKey"] = lek
            resp = table.query(**kwargs)
            items.extend(resp.get("Items", []))
            lek = resp.get("LastEvaluatedKey")
            if not lek: break
        items = [it for it in items if it.get("SK")=="PROFILE" and it.get("Type")=="Device" and it.get("Status")!="deleted"]
    else:
        lek = None
        while True:
            resp = table.scan(FilterExpression=fe, ExclusiveStartKey=lek or None)
            items.extend(resp.get("Items", []))
            lek = resp.get("LastEvaluatedKey")
            if not lek: break

    if q:
        items = [it for it in items if q in str(it.get("Brand","")).lower()
                               or q in str(it.get("Model","")).lower()
                               or q in str(it.get("Variant","")).lower()]
    if active_s in ("true","false"):
        want = (active_s == "true")
        items = [it for it in items if bool(it.get("Active", True)) == want]
    return items

# --- Export endpoints -------------------------------------------------------

@bp.get("/export/table")
@require_role("admin")
def export_table_xlsx():
    """One worksheet that matches the filtered table."""
    items = _fetch_filtered_devices_for_export()
    bio = _xlsx_from_items(items, grouped=False)
    return send_file(bio, as_attachment=True, download_name="devices.xlsx",
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

@bp.get("/export/grouped")
@require_role("admin")
def export_grouped_xlsx():
    """Three worksheets: Laptop / Smartphone / Tablet (filters still applied)."""
    items = _fetch_filtered_devices_for_export()
    bio = _xlsx_from_items(items, grouped=True)
    return send_file(bio, as_attachment=True, download_name="devices_grouped.xlsx",
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
