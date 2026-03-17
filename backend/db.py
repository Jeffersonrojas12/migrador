"""Módulo de base de datos — SQLite (local) o PostgreSQL (Supabase/Railway)"""
import os, threading

BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
DATABASE_URL = os.environ.get('DATABASE_URL', '')

if DATABASE_URL:
    # ── PostgreSQL ──────────────────────────────────────────────
    import psycopg2
    from psycopg2.extras import RealDictCursor
    _pg_lock = threading.Lock()
    _pg_conn  = None
    DB_PATH   = DATABASE_URL.split('@')[-1]

    def get_db():
        global _pg_conn
        with _pg_lock:
            if _pg_conn is None or _pg_conn.closed:
                _pg_conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
                _pg_conn.autocommit = False
        return _pg_conn

else:
    # ── SQLite ──────────────────────────────────────────────────
    import sqlite3
    DATA_DIR = os.environ.get('RAILWAY_VOLUME_MOUNT_PATH', BASE_DIR)
    DB_PATH  = os.path.join(DATA_DIR, 'wo_migrador.db')
    _db_local = threading.local()

    def get_db():
        if not hasattr(_db_local, 'conn') or _db_local.conn is None:
            conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA synchronous=NORMAL")
            _db_local.conn = conn
        return _db_local.conn
