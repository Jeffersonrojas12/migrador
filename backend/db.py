"""Módulo de base de datos compartido"""
import os, sqlite3, threading

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH  = os.path.join(BASE_DIR, 'wo_migrador.db')

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
