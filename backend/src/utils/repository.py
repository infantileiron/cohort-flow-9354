import json, sqlite3
from dataclasses import asdict
from fastapi import HTTPException
from src.config.settings import DATABASE_URL
TABLES = ("datasets", "cohorts", "runs", "reviews", "reports", "users", "audit")
def connect():
    if DATABASE_URL.startswith("sqlite:///"):
        # timeout is the cross-process lock wait; BEGIN IMMEDIATE below serialises claims.
        conn = sqlite3.connect(DATABASE_URL.removeprefix("sqlite:///"), timeout=30)
        return conn, "?"
    import psycopg
    return psycopg.connect(DATABASE_URL), "%s"
def init_db():
    conn,_ = connect()
    try:
        if not DATABASE_URL.startswith("sqlite:///"):
            conn.execute("SELECT pg_advisory_xact_lock(2201)")
        for table in TABLES:
            conn.execute(f"CREATE TABLE IF NOT EXISTS {table} (id TEXT PRIMARY KEY, payload TEXT NOT NULL)")
        conn.commit()
    finally: conn.close()
def all_rows(table):
    assert table in TABLES
    conn,_ = connect()
    try: return [json.loads(row[0]) for row in conn.execute(f"SELECT payload FROM {table} ORDER BY id").fetchall()]
    finally: conn.close()
def get(table, id):
    assert table in TABLES
    conn,q = connect()
    try:
        row = conn.execute(f"SELECT payload FROM {table} WHERE id={q}", (id,)).fetchone()
        if row is None: raise HTTPException(404, "记录不存在")
        return json.loads(row[0])
    finally: conn.close()
def put(table, item, insert_only=False):
    assert table in TABLES
    if not isinstance(item, dict): item = asdict(item)
    conn,q = connect()
    clause = "DO NOTHING" if insert_only else "DO UPDATE SET payload=excluded.payload"
    try:
        conn.execute(f"INSERT INTO {table} (id,payload) VALUES ({q},{q}) ON CONFLICT(id) {clause}", (item["id"], json.dumps(item, ensure_ascii=False, allow_nan=False)))
        conn.commit()
        return item
    finally: conn.close()
def mutate(table, id, fn):
    """Atomically load, transform and store one row.

    ``fn(item_or_none) -> (new_item_or_none, outcome)`` runs inside a database
    transaction holding a row lock (SQLite: BEGIN IMMEDIATE, PostgreSQL:
    SELECT ... FOR UPDATE).  Returning ``None`` as the new item skips the write,
    which lets callers use this as a compare-and-swap for job claims and for
    fencing stale workers.  Returns ``fn``'s outcome.
    """
    assert table in TABLES
    conn,q = connect()
    sqlite = DATABASE_URL.startswith("sqlite:///")
    try:
        lock = "" if sqlite else " FOR UPDATE"
        if sqlite:
            conn.isolation_level = None
            conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(f"SELECT payload FROM {table} WHERE id={q}{lock}", (id,)).fetchone()
        item = json.loads(row[0]) if row else None
        # PostgreSQL: a missing row cannot be locked, so serialise concurrent
        # first-inserts of the same id with a transaction-scoped advisory lock.
        if not sqlite and item is None:
            conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"{table}:{id}",))
            row = conn.execute(f"SELECT payload FROM {table} WHERE id={q}{lock}", (id,)).fetchone()
            item = json.loads(row[0]) if row else None
        new_item, outcome = fn(item)
        if new_item is not None:
            conn.execute(f"INSERT INTO {table} (id,payload) VALUES ({q},{q}) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload", (new_item["id"], json.dumps(new_item, ensure_ascii=False, allow_nan=False)))
        conn.commit()
        return outcome
    except Exception:
        conn.rollback()
        raise
    finally: conn.close()
