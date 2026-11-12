"""
Simple CDC sync: Azure MySQL -> Azure MySQL (mixed PK and non-PK tables)

How it works
  1) Read the source (gphinblue-poc) binlog (ROW format) as cdc_reader.
  2) Receive INSERT / UPDATE / DELETE events.
     - With ROW format + binlog_row_image=FULL, each event carries the full
       before/after column values.
     - Locate target rows by the configured PK, or a NOT NULL UNIQUE KEY
       for tables without a PK. Composite keys are supported.
  3) Apply the changes to the target (gphingreen-poc) as cdc_writer.
  4) Save the last processed binlog position (file, pos) to a file,
     so a restart resumes where it left off.

Install:  pip install -r requirements.txt
Configuration: .env beside this script contains DB and SRC_/DST_ HOST, USER,
PASSWORD, and PORT settings. Existing environment variables take precedence.
Table keys: configure PRIMARY_KEY for PK tables and UNIQUE_KEY for non-PK
tables to match the identical source/target schemas. PK takes precedence.
See samplescript.sql for setup and mixed-table INSERT/UPDATE/DELETE tests.
"""
import json
import os
from pathlib import Path

import certifi
import pymysql
from dotenv import load_dotenv
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent

load_dotenv(Path(__file__).with_name(".env"), interpolate=False)


def required_env(name):
    value = os.environ.get(name)
    if value is None or value == "":
        raise ValueError(f"Missing required setting: {name}. Set it in .env or the environment.")
    return value


def env_port(name):
    try:
        port = int(required_env(name))
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer between 1 and 65535.") from exc
    if not 1 <= port <= 65535:
        raise ValueError(f"{name} must be an integer between 1 and 65535.")
    return port


# ───────────── Settings ─────────────
DB = required_env("DB")

SRC = dict(host=required_env("SRC_HOST"), port=env_port("SRC_PORT"),
           user=required_env("SRC_USER"), password=required_env("SRC_PASSWORD"))
DST = dict(host=required_env("DST_HOST"), port=env_port("DST_PORT"),
           user=required_env("DST_USER"), password=required_env("DST_PASSWORD"), database=DB)

# Azure MySQL requires SSL by default.
# certifi ships a CA bundle that includes DigiCert Global Root G2 and Microsoft RSA Root CA 2017,
# so no manual cert download is needed. (Or use your own file: SSL = {"ca": r"C:\certs\azure-ca.pem"})
SSL = {"ca": certifi.where()}

# Explicit schema configuration; key columns must match source and target.
PRIMARY_KEY = {
    "product": ("product_id",),
    "order_item": ("order_id", "line_no"),
}
UNIQUE_KEY = {
    "customer": ("customer_no",),
    "orders": ("order_no",),
}
ROW_KEY = {**UNIQUE_KEY, **PRIMARY_KEY}

STATE_FILE = "cdc_state.json"   # stores the last processed binlog position
SERVER_ID = 4321                # replication client ID (must not collide with the source server or other clients)
# ────────────────────────────────────


def load_state():
    """Read the saved binlog position. If none, start from the source's current position."""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                return json.load(f)
        except json.JSONDecodeError:
            # Empty or corrupted state file -> ignore it and start from the current position
            print(f"WARNING: {STATE_FILE} is empty or invalid, ignoring it.")
    conn = pymysql.connect(**SRC, ssl=SSL)
    with conn.cursor() as cur:
        cur.execute("SHOW MASTER STATUS")
        row = cur.fetchone()
    conn.close()
    return {"file": row[0], "pos": row[1]}


def save_state(file, pos):
    # Write to a temp file, then replace, so a crash never leaves a half-written state file
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"file": file, "pos": pos}, f)
    os.replace(tmp, STATE_FILE)


def quote_identifier(name):
    return "`" + name.replace("`", "``") + "`"


def key_values(table, columns, values):
    for column in columns:
        if column not in values or values[column] is None:
            raise ValueError(f"{table}: key column {column} is missing or NULL; "
                             "check key configuration and binlog_row_image=FULL.")
    return [values[column] for column in columns]


def apply_event(cur, event):
    """Apply one binlog event to the target."""
    table = event.table
    keys = ROW_KEY.get(table)
    if keys is None:
        return  # ignore tables that are not configured
    table_sql = quote_identifier(table)
    where = " AND ".join(f"{quote_identifier(key)}=%s" for key in keys)

    for row in event.rows:
        # -- INSERT --  overwrite if the row already exists (safe to replay)
        if isinstance(event, WriteRowsEvent):
            vals = row["values"]
            key_values(table, keys, vals)
            cols = ", ".join(quote_identifier(c) for c in vals)
            ph = ", ".join(["%s"] * len(vals))
            upd = ", ".join(f"{quote_identifier(c)}=VALUES({quote_identifier(c)})" for c in vals)
            cur.execute(
                f"INSERT INTO {table_sql} ({cols}) VALUES ({ph}) ON DUPLICATE KEY UPDATE {upd}",
                list(vals.values()),
            )

        # Use the old key so changing a PK or UNIQUE KEY still locates the row.
        elif isinstance(event, UpdateRowsEvent):
            before, after = row["before_values"], row["after_values"]
            old_key = key_values(table, keys, before)
            key_values(table, keys, after)
            sets = ", ".join(f"{quote_identifier(c)}=%s" for c in after)
            cur.execute(
                f"UPDATE {table_sql} SET {sets} WHERE {where}",
                list(after.values()) + old_key,
            )

        # -- DELETE --  delete by the configured PK or UNIQUE KEY
        elif isinstance(event, DeleteRowsEvent):
            cur.execute(f"DELETE FROM {table_sql} WHERE {where}",
                        key_values(table, keys, row["values"]))


def main():
    state = load_state()
    print(f"Starting position: {state['file']} / {state['pos']}")

    stream = BinLogStreamReader(
        connection_settings={**SRC, "ssl": SSL},
        server_id=SERVER_ID,
        only_schemas=[DB],
        only_tables=list(ROW_KEY),
        only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
        log_file=state["file"],
        log_pos=state["pos"],
        resume_stream=True,
        blocking=True,          # wait for new changes (runs continuously)
    )

    dst = pymysql.connect(**DST, ssl=SSL, autocommit=False)
    try:
        for event in stream:
            with dst.cursor() as cur:
                apply_event(cur, event)
            dst.commit()                                 # apply to target first
            save_state(stream.log_file, stream.log_pos)  # then save the position
            print(f"[{event.table}] {type(event).__name__} {len(event.rows)} row(s) -> {stream.log_file}:{stream.log_pos}")
    finally:
        stream.close()
        dst.close()


if __name__ == "__main__":
    main()