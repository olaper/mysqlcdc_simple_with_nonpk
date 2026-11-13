# MySQL CDC Sample

A simple, one-way Change Data Capture (CDC) demo between two Azure Database for
MySQL servers. [cdc_sync.py](cdc_sync.py) reads source binlog events and applies
INSERT, UPDATE, and DELETE changes to the target.

The sample mixes tables with and without primary keys:

| Table | Row identification |
|---|---|
| `product` | Primary key: `product_id` |
| `order_item` | Composite primary key: `order_id`, `line_no` |
| `customer` | No PK; UNIQUE KEY: `customer_no` |
| `orders` | No PK; UNIQUE KEY: `order_no` |
| `activity_log` | No declared PK or UNIQUE KEY; MySQL generates invisible PK `my_row_id` |

Keys are explicitly configured in `PRIMARY_KEY` and `UNIQUE_KEY` in the script,
not detected automatically. UPDATE and DELETE locate rows using these keys;
UPDATE uses the old key when a key value changes. INSERT uses an upsert.

## Prerequisites

- Python and network access to both MySQL servers.
- Source binary logging enabled with `binlog_format=ROW` and
  `binlog_row_image=FULL`.
- Source user with `SELECT`, `REPLICATION SLAVE`, and `REPLICATION CLIENT`;
  target user with `SELECT`, `INSERT`, `UPDATE`, and `DELETE`.
  Use an administrative account to create the sample schema.
- Identical source/target table definitions. Non-PK key columns must be
  `NOT NULL` and covered by a UNIQUE index.
- For the no-PK tables, set `sql_require_primary_key=OFF` on both servers.
- MySQL 8.0.30+ on both servers for Generated Invisible Primary Keys (GIPK).
  Setup disables session `sql_generate_invisible_primary_key` for the original
  tables, enables it only when creating `activity_log` with InnoDB, then restores
  the original setting. Run STEP 1 in a single connection on each server.
  A GIPK is a real primary key, but is not explicitly declared in the sample DDL.
  CDC explicitly copies `my_row_id` from the source rather than generating a
  separate target identity. `SELECT *` hides it; select `my_row_id` explicitly
  when comparing source and target.

## Setup

Run these commands in PowerShell from the project directory:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
Copy-Item .env.example .env
```

Skip the copy command if you already have a configured `.env`.
Fill in all placeholders from [.env.example](.env.example): database name,
source/target hostnames, usernames, passwords, and numeric ports (usually `3306`).
Set `DB` to `ghbin_sampledb2`, or adjust the database name in
[samplescript.sql](samplescript.sql) to match your choice.

The script loads `.env` beside itself; existing environment variables take
precedence. TLS uses the CA bundle supplied by `certifi`. Do not commit `.env`.

## Run the CDC Test

**Use disposable test databases. STEP 1 drops the sample tables and their data.
Execute the SQL file section by section, not all at once.**

1. Stop CDC if it is running. Execute **STEP 1** of
   [samplescript.sql](samplescript.sql) on **both source and target**.
2. For a fresh test with recreated, empty tables, move any existing
   `cdc_state.json` out of the project directory. Keep it for ordinary restarts.
3. Start CDC from the project directory:

   ```powershell
   .\.venv\Scripts\python.exe -u .\cdc_sync.py
   ```

4. Wait for `Starting position`, then execute **STEP 2** and **STEP 3** on
   the **source only** while CDC remains running. These cover inserts,
   normal updates, PK/UNIQUE KEY changes, multi-row updates, and deletes.
5. After CDC catches up, execute **STEP 4** on **both servers** and compare
   the full query results. Expected final row counts:

   | Table | Rows |
   |---|---|
   | `customer` | 2 |
   | `orders` | 2 |
   | `product` | 2 |
   | `order_item` | 3 |
   | `activity_log` | 2 |

   Expected key changes include `C002X`, `O1002X`, product ID `202`, and
   order-item key `(1001, 3)`. More expected values are listed in the SQL file.
6. Stop with **Ctrl+C**. Restart from the same directory with the same
   checkpoint to resume; make additional source changes to check continued sync.

## Run Unit Tests

```powershell
.\.venv\Scripts\python.exe -m unittest -v test_cdc_sync
```

These tests check SQL generation for single/composite PKs, GIPKs, and UNIQUE keys,
key changes, missing keys, and stream table selection without connecting to MySQL.
They also check checkpoint creation, atomic replacement, retries, and preservation
of the previous checkpoint when replacement fails.

## Checkpoint Access Errors on Windows

Checkpoint writes use a temporary file followed by atomic replacement. Temporary
`PermissionError` failures during replacement are retried up to five attempts
with short backoff and warnings. Persistent failures stop CDC with an error;
they are not treated as successful checkpoint saves.

If replacement fails, check read-only attributes and Modify permissions on
[cdc_state.json](cdc_state.json) and its directory, and check for other programs
holding the file open without delete sharing. Run only one CDC instance per
checkpoint; replacement retries do not make concurrent instances safe.

The target event is committed before the checkpoint is saved. Keep the existing
checkpoint and the remaining temporary file for diagnosis; do not delete the
checkpoint or promote the temporary file blindly. After resolving the access
problem, restart with the existing checkpoint. Some committed events may replay;
this demo does not guarantee replay safety for every sequence of key changes.

## Sample Limitations

- No initial snapshot: without a checkpoint, CDC starts at the source's current
  binlog position. Existing rows need a separate consistent bootstrap.
- DDL changes are not replicated; manage schema changes on both servers.
- This is a demo, not a production replication system. It commits per row event
  and saves the checkpoint separately, so it does not provide atomic source
  transaction replication or exactly-once delivery.
