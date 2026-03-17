"""Módulo de base de datos — wrapper unificado SQLite/PostgreSQL"""
import os, threading, sqlite3

BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
DATABASE_URL = os.environ.get('DATABASE_URL', '')

if DATABASE_URL:
    DB_PATH = DATABASE_URL.split('@')[-1]
else:
    DATA_DIR = os.environ.get('RAILWAY_VOLUME_MOUNT_PATH', BASE_DIR)
    DB_PATH  = os.path.join(DATA_DIR, 'wo_migrador.db')

_db_local = threading.local()

class PGWrapper:
    """Wraps psycopg2 connection to behave like sqlite3 for simple queries"""
    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, params=None):
        sql = sql.replace('?', '%s')
        cur = self._conn.cursor()
        cur.execute(sql, params or ())
        return cur

    def executescript(self, script):
        cur = self._conn.cursor()
        for stmt in script.strip().split(';'):
            stmt = stmt.strip()
            if stmt:
                try: cur.execute(stmt)
                except Exception as e:
                    if 'already exists' not in str(e).lower():
                        print(f"[DB] warning: {e}")
                    self._conn.rollback()
        return cur

    def commit(self): self._conn.commit()
    def rollback(self): self._conn.rollback()

    @property
    def closed(self): return self._conn.closed

def get_db():
    if DATABASE_URL:
        import psycopg2
        from psycopg2.extras import RealDictCursor
        if not hasattr(_db_local, 'conn') or _db_local.conn is None or _db_local.conn.closed:
            conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
            conn.autocommit = False
            _db_local.conn = PGWrapper(conn)
        return _db_local.conn
    else:
        if not hasattr(_db_local, 'conn') or _db_local.conn is None:
            conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA synchronous=NORMAL")
            _db_local.conn = conn
        return _db_local.conn
