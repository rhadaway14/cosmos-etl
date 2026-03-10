#!/usr/bin/env python3
"""
ETL: XML -> compact unique-fields JSON -> Azure Cosmos DB (SQL API)

Usage:
  python main.py --input ./xml_dir --pattern "*.xml" --dry-run
  python main.py --input ./xml_dir --pattern "*.xml"

Notes:
- "id" priority order:
    1) CF3461Block4 (with BII- prefix removed if present)
    2) EntryNum
    3) SBNum
    4) UUID4 fallback
"""

import argparse
import json
import sys
import uuid
from collections import Counter, defaultdict
from pathlib import Path
import xml.etree.ElementTree as ET

# ---- Cosmos (SQL API) SDK
COSMOS_IMPORT_ERR = """\
The Azure Cosmos SDK for Python is not installed.
Install it first with:
    pip install azure-cosmos
"""
try:
    from azure.cosmos import CosmosClient
except Exception:
    CosmosClient = None


# ---------------------------
# Utility Functions
# ---------------------------

def clean_text(t: str) -> str:
    if t is None:
        return ""
    return " ".join(t.split())


def xml_to_compact_unique_json(xml_text: str) -> dict:
    root = ET.fromstring(xml_text)

    rows = []
    for elem in root.iter():
        if elem is root:
            continue

        tag = elem.tag.split("}")[-1]
        raw = elem.text if elem.text is not None else ""
        text = clean_text(raw)

        keep = bool(text) or raw in ("0", "false", "true")
        if keep:
            if not text and raw in ("0", "false", "true"):
                text = raw
            rows.append((tag, text))

    ctr = Counter(rows)
    unique_pairs = [(k[0], k[1]) for k, c in ctr.items() if c == 1]

    grouped = defaultdict(list)
    for field, value in unique_pairs:
        grouped[field].append(value)

    return {k: (v[0] if len(v) == 1 else v) for k, v in sorted(grouped.items())}


def sanitize_id(value: str) -> str:
    """
    Cosmos DB id cannot contain '/', '\\', '?', '#'
    """
    if value is None:
        return ""
    s = str(value).strip()
    if not s:
        return ""
    for ch in ("/", "\\", "?", "#"):
        s = s.replace(ch, "_")
    return s


def transform_cf3461(value: str) -> str:
    """
    Remove 'BII-' prefix if present.
    Example:
        BII-0137190-5  ->  0137190-5
    """
    if not value:
        return ""
    value = str(value).strip()
    if value.upper().startswith("BII-"):
        return value[4:]
    return value


def choose_id(doc: dict) -> str:
    """
    ID selection order:
        1) CF3461Block4 (strip BII-)
        2) EntryNum
        3) SBNum
        4) UUID fallback
    """
    cf_value = doc.get("CF3461Block4")

    if isinstance(cf_value, list):
        for item in cf_value:
            candidate = sanitize_id(transform_cf3461(str(item)))
            if candidate:
                return candidate
    elif cf_value is not None:
        candidate = sanitize_id(transform_cf3461(str(cf_value)))
        if candidate:
            return candidate

    entry = doc.get("EntryNum")
    if isinstance(entry, list):
        for item in entry:
            candidate = sanitize_id(str(item))
            if candidate:
                return candidate
    elif entry is not None:
        candidate = sanitize_id(str(entry))
        if candidate:
            return candidate

    sb = doc.get("SBNum")
    if isinstance(sb, list):
        for item in sb:
            candidate = sanitize_id(str(item))
            if candidate:
                return candidate
    elif sb is not None:
        candidate = sanitize_id(str(sb))
        if candidate:
            return candidate

    return str(uuid.uuid4())


# ---------------------------
# Cosmos Upsert
# ---------------------------

# def upsert_to_cosmos(items, dry_run=False):
#     # uri = ""
#     uri = ""
#     db_name = "ryker"
#     container_name = "items"
#     pk_field = "pk"
#
#     if dry_run:
#         print(
#             f"[DRY-RUN] Would upsert {len(items)} item(s) "
#             f"to Cosmos DB '{db_name}/{container_name}' "
#             f"with PK field '{pk_field}'."
#         )
#         return
#
#     if not CosmosClient:
#         print(COSMOS_IMPORT_ERR, file=sys.stderr)
#         sys.exit(2)
#
#     try:
#         client = CosmosClient(uri, credential=key)
#         db = client.get_database_client(db_name)
#         container = db.get_container_client(container_name)
#
#         props = container.read()
#         print("\n--- Cosmos Container Info ---")
#         print(f"Database: {db_name}")
#         print(f"Container: {container_name}")
#         print(f"Container partition key definition: {props.get('partitionKey')}")
#         print("-----------------------------\n")
#
#     except Exception as e:
#         print(f"Failed to connect to Cosmos DB: {type(e).__name__}: {e}", file=sys.stderr)
#         sys.exit(2)
#
#     success = 0
#     failed = 0
#
#     for i, doc in enumerate(items, start=1):
#         try:
#             if pk_field not in doc or not doc.get(pk_field):
#                 doc[pk_field] = doc["id"]
#
#             print(f"[{i}/{len(items)}] Upserting id={doc.get('id')} pk={doc.get(pk_field)}")
#
#             container.upsert_item(doc)
#             success += 1
#
#         except Exception as e:
#             failed += 1
#             print("\n=== UPSERT FAILED ===", file=sys.stderr)
#             print(f"Index: {i}", file=sys.stderr)
#             print(f"id: {doc.get('id')}", file=sys.stderr)
#             print(f"pk: {doc.get(pk_field)}", file=sys.stderr)
#             print(f"Exception type: {type(e).__name__}", file=sys.stderr)
#             print(f"Exception: {e}", file=sys.stderr)
#             print(f"Document keys: {list(doc.keys())[:25]}", file=sys.stderr)
#
#             try:
#                 short_doc = {k: doc[k] for k in list(doc.keys())[:10]}
#                 print("Document sample:", json.dumps(short_doc, indent=2)[:2000], file=sys.stderr)
#             except Exception:
#                 pass
#
#             print("=====================\n", file=sys.stderr)
#
#     print(f"Upsert complete: {success}/{len(items)} succeeded, {failed} failed.")


def upsert_to_cosmos(items, dry_run=False):
    # uri = ""
    # key = ""
    db_name = "ryker"
    container_name = "ryker"
    pk_field = "pk"

    if dry_run:
        print(
            f"[DRY-RUN] Would upsert {len(items)} item(s) "
            f"to Cosmos DB '{db_name}/{container_name}' "
            f"with PK field '{pk_field}'."
        )
        return

    if not CosmosClient:
        print(COSMOS_IMPORT_ERR, file=sys.stderr)
        sys.exit(2)

    try:
        client = CosmosClient(uri, credential=key)

        print("\n--- Connected to Cosmos account ---")
        print(f"URI: {uri}")
        print("----------------------------------\n")

        # List databases
        dbs = list(client.list_databases())
        db_ids = [d.get("id") for d in dbs]
        print("Available databases:")
        for dbid in db_ids:
            print(f"  - {dbid}")

        if db_name not in db_ids:
            print(f"\nERROR: Database '{db_name}' was not found.", file=sys.stderr)
            sys.exit(2)

        db = client.get_database_client(db_name)

        # List containers in selected database
        containers = list(db.list_containers())
        container_ids = [c.get("id") for c in containers]
        print(f"\nAvailable containers in database '{db_name}':")
        for cid in container_ids:
            print(f"  - {cid}")

        if container_name not in container_ids:
            print(f"\nERROR: Container '{container_name}' was not found in database '{db_name}'.", file=sys.stderr)
            sys.exit(2)

        container = db.get_container_client(container_name)
        props = container.read()

        print("\n--- Cosmos Container Info ---")
        print(f"Database: {db_name}")
        print(f"Container: {container_name}")
        print(f"Container partition key definition: {props.get('partitionKey')}")
        print("-----------------------------\n")

    except Exception as e:
        print(f"Failed to connect to Cosmos DB: {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(2)

    success = 0
    failed = 0

    for i, doc in enumerate(items, start=1):
        try:
            if pk_field not in doc or not doc.get(pk_field):
                doc[pk_field] = doc["id"]

            print(f"[{i}/{len(items)}] Upserting id={doc.get('id')} pk={doc.get(pk_field)}")
            container.upsert_item(doc)
            success += 1

        except Exception as e:
            failed += 1
            print("\n=== UPSERT FAILED ===", file=sys.stderr)
            print(f"Index: {i}", file=sys.stderr)
            print(f"id: {doc.get('id')}", file=sys.stderr)
            print(f"pk: {doc.get(pk_field)}", file=sys.stderr)
            print(f"Exception type: {type(e).__name__}", file=sys.stderr)
            print(f"Exception: {e}", file=sys.stderr)
            print("=====================\n", file=sys.stderr)

    print(f"Upsert complete: {success}/{len(items)} succeeded, {failed} failed.")

# ---------------------------
# Main
# ---------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Directory (or file) containing XMLs.")
    ap.add_argument("--pattern", default="*.xml", help="Glob pattern if directory.")
    ap.add_argument("--output", default=None, help="Optional folder to write JSON files.")
    ap.add_argument("--dry-run", action="store_true", help="Skip Cosmos upsert.")
    args = ap.parse_args()

    input_path = Path(args.input)

    if input_path.is_dir():
        xml_paths = sorted(input_path.glob(args.pattern))
    else:
        xml_paths = [input_path]

    if not xml_paths:
        print("No XML files found.", file=sys.stderr)
        sys.exit(1)

    out_dir = None
    if args.output:
        out_dir = Path(args.output)
        out_dir.mkdir(parents=True, exist_ok=True)

    items = []

    for p in xml_paths:
        try:
            xml_text = p.read_text(encoding="utf-8", errors="ignore")
            doc = xml_to_compact_unique_json(xml_text)

            doc_id = choose_id(doc)
            doc["id"] = str(doc_id)

            if out_dir:
                out_path = out_dir / f"{p.stem}.json"
                out_path.write_text(json.dumps(doc, indent=2), encoding="utf-8")

            doc_json = json.dumps(doc)
            print(
                f"Transformed: {p.name} -> id={doc['id']} "
                f"({len(doc)} fields, {len(doc_json.encode('utf-8'))} bytes)"
            )

            items.append(doc)

        except Exception as e:
            print(f"Error processing {p}: {type(e).__name__}: {e}", file=sys.stderr)

    upsert_to_cosmos(items, dry_run=args.dry_run)


if __name__ == "__main__":
    main()