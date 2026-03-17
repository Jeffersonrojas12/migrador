"""Gunicorn config — runs init_db() once before workers start"""
import os

bind = f"0.0.0.0:{os.environ.get('PORT', '8080')}"
workers = 2

def on_starting(server):
    """Runs once before workers fork — safe for DB init"""
    import sys
    sys.path.insert(0, os.path.dirname(__file__))
    from app import app, init_db
    with app.app_context():
        init_db()
