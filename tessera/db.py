"""PostgreSQL / TimescaleDB access with reconnect-and-retry (the DB may restart under us)."""
import datetime
import decimal
import json
import logging
import os
import time

import psycopg2
import psycopg2.extras

from . import config

log = logging.getLogger("tessera.db")

SCHEMA_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "db", "init.sql")


class DB:
    def __init__(self, dsn=None):
        self.dsn = dsn or config.PG_DSN
        self.conn = None

    def connect(self, timeout_s=180):
        t0 = time.time()
        while True:
            try:
                self.conn = psycopg2.connect(self.dsn, connect_timeout=5)
                self.conn.autocommit = False
                return self
            except psycopg2.OperationalError as e:
                if time.time() - t0 > timeout_s:
                    raise
                log.info("waiting for database (%s)", str(e).strip().splitlines()[0])
                time.sleep(3)

    def ensure_schema(self):
        if not os.path.exists(SCHEMA_FILE):
            return
        with open(SCHEMA_FILE, encoding="utf-8") as f:
            sql = f.read()
        self.run(lambda cur: cur.execute(sql))

    def run(self, fn, retries=5):
        """Run fn(cursor) in one transaction; reconnect and retry on connection loss."""
        for attempt in range(retries):
            try:
                if self.conn is None or self.conn.closed:
                    self.connect()
                with self.conn.cursor() as cur:
                    out = fn(cur)
                self.conn.commit()
                return out
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                log.warning("db connection problem (%s), retry %d", e, attempt + 1)
                try:
                    self.conn.close()
                except Exception:  # noqa: BLE001
                    pass
                self.conn = None
                time.sleep(min(2 ** attempt, 10))
            except Exception:
                if self.conn is not None and not self.conn.closed:
                    self.conn.rollback()
                raise
        raise RuntimeError("database unavailable after retries")

    def query(self, sql, params=None):
        def fn(cur):
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return [{c: _plain(v) for c, v in zip(cols, row)} for row in cur.fetchall()]
        return self.run(fn)

    def execute(self, sql, params=None):
        return self.run(lambda cur: cur.execute(sql, params))

    def execute_values(self, sql, rows, template=None):
        if not rows:
            return
        self.run(lambda cur: psycopg2.extras.execute_values(cur, sql, rows, template=template, page_size=500))


def _plain(v):
    if isinstance(v, decimal.Decimal):
        return float(v)
    if isinstance(v, (datetime.datetime, datetime.date)):
        return v.isoformat()
    return v


def jsonb(obj):
    return psycopg2.extras.Json(obj, dumps=lambda o: json.dumps(o, default=str))
