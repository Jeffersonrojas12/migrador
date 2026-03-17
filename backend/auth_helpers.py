"""Módulo de autenticación: hashing, decoradores JWT"""
import hashlib, hmac, secrets
from functools import wraps
from flask import request, jsonify, g, current_app
from db import get_db

try:
    import jwt as pyjwt
except ImportError:
    import PyJWT as pyjwt


def hash_password(p):
    salt = secrets.token_hex(16)
    h = hashlib.pbkdf2_hmac('sha256', p.encode(), salt.encode(), 260_000)
    return f"{salt}:{h.hex()}"


def verify_password(p, stored):
    try:
        salt, h = stored.split(':', 1)
        exp = hashlib.pbkdf2_hmac('sha256', p.encode(), salt.encode(), 260_000)
        return hmac.compare_digest(exp, bytes.fromhex(h))
    except:
        return False


def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        header = request.headers.get('Authorization', '')
        if not header.startswith('Bearer '):
            return jsonify(error='Token requerido'), 401
        token = header[7:]
        try:
            payload = pyjwt.decode(token, current_app.config['SECRET_KEY'], algorithms=['HS256'])
        except pyjwt.ExpiredSignatureError:
            return jsonify(error='Sesión expirada'), 401
        except pyjwt.InvalidTokenError:
            return jsonify(error='Token inválido'), 401
        db = get_db()
        ses = db.execute("SELECT * FROM sessions WHERE token_jti=? AND active=1", (payload['jti'],)).fetchone()
        if not ses: return jsonify(error='Sesión cerrada'), 401
        user = db.execute("SELECT * FROM users WHERE id=? AND active=1", (payload['sub'],)).fetchone()
        if not user: return jsonify(error='Usuario no encontrado'), 401
        g.user = dict(user); g.token_jti = payload['jti']
        return f(*args, **kwargs)
    return decorated


def require_admin(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        header = request.headers.get('Authorization', '')
        if not header.startswith('Bearer '):
            return jsonify(error='Token requerido'), 401
        token = header[7:]
        try:
            payload = pyjwt.decode(token, current_app.config['SECRET_KEY'], algorithms=['HS256'])
        except pyjwt.ExpiredSignatureError:
            return jsonify(error='Sesión expirada'), 401
        except pyjwt.InvalidTokenError:
            return jsonify(error='Token inválido'), 401
        db = get_db()
        ses = db.execute("SELECT * FROM sessions WHERE token_jti=? AND active=1", (payload['jti'],)).fetchone()
        if not ses: return jsonify(error='Sesión cerrada'), 401
        user = db.execute("SELECT * FROM users WHERE id=? AND active=1", (payload['sub'],)).fetchone()
        if not user: return jsonify(error='Usuario no encontrado'), 401
        g.user = dict(user); g.token_jti = payload['jti']
        if g.user['role'] != 'admin':
            return jsonify(error='Se requiere rol administrador'), 403
        return f(*args, **kwargs)
    return decorated
