"""
Módulo: Gestión de Usuarios
Rutas: /api/users  (GET, POST)
       /api/users/<uid>  (GET, PUT, DELETE)
"""
import re
import sqlite3
from flask import Blueprint, request, jsonify, g
from db import get_db
from auth_helpers import require_auth, require_admin, hash_password

usuarios_bp = Blueprint('usuarios', __name__)


@usuarios_bp.route('/api/users', methods=['GET'])
@require_admin
def list_users():
    db = get_db()
    rows = db.execute(
        "SELECT id,email,name,initials,phone,role,active,created_at,last_login FROM users ORDER BY id"
    ).fetchall()
    return jsonify([dict(r) for r in rows])


@usuarios_bp.route('/api/users', methods=['POST'])
@require_admin
def create_user():
    d        = request.get_json(force=True) or {}
    email    = (d.get('email') or '').strip().lower()
    password = d.get('password') or ''
    name     = (d.get('name') or '').strip()
    phone    = (d.get('phone') or '').strip()
    role     = d.get('role', 'user')
    if not email or not password or not name:
        return jsonify(error='Email, contraseña y nombre son requeridos'), 400
    if not re.match(r'^[^@]+@[^@]+\.[^@]+$', email):
        return jsonify(error='Email inválido'), 400
    if role not in ('admin', 'user'):
        return jsonify(error='Rol inválido'), 400
    initials = ''.join(w[0].upper() for w in name.split()[:2])
    db = get_db()
    try:
        db.execute(
            "INSERT INTO users (email,password_hash,name,initials,phone,role) VALUES (?,?,?,?,?,?)",
            (email, hash_password(password), name, initials, phone, role)
        )
        db.commit()
    except sqlite3.IntegrityError:
        return jsonify(error='El correo ya está registrado'), 409
    user = db.execute(
        "SELECT id,email,name,initials,phone,role,active,created_at FROM users WHERE email=?", (email,)
    ).fetchone()
    return jsonify(dict(user)), 201


@usuarios_bp.route('/api/users/<int:uid>', methods=['GET'])
@require_auth
def get_user(uid):
    if g.user['role'] != 'admin' and g.user['id'] != uid:
        return jsonify(error='Sin permiso'), 403
    db = get_db()
    user = db.execute(
        "SELECT id,email,name,initials,phone,role,active,created_at,last_login FROM users WHERE id=?", (uid,)
    ).fetchone()
    if not user:
        return jsonify(error='Usuario no encontrado'), 404
    return jsonify(dict(user))


@usuarios_bp.route('/api/users/<int:uid>', methods=['PUT'])
@require_admin
def update_user(uid):
    d    = request.get_json(force=True) or {}
    name = (d.get('name') or '').strip()
    if not name:
        return jsonify(error='Nombre requerido'), 400
    db = get_db()
    db.execute("UPDATE users SET name=?,phone=?,role=?,active=? WHERE id=?",
               (name, d.get('phone',''), d.get('role','user'), int(d.get('active',1)), uid))
    if d.get('password'):
        db.execute("UPDATE users SET password_hash=? WHERE id=?",
                   (hash_password(d['password']), uid))
    db.commit()
    user = db.execute(
        "SELECT id,email,name,initials,phone,role,active FROM users WHERE id=?", (uid,)
    ).fetchone()
    return jsonify(dict(user))


@usuarios_bp.route('/api/users/<int:uid>', methods=['DELETE'])
@require_admin
def deactivate_user(uid):
    if uid == g.user['id']:
        return jsonify(error='No puedes desactivar tu propia cuenta'), 400
    db = get_db()
    db.execute("UPDATE users SET active=0 WHERE id=?", (uid,))
    db.commit()
    return jsonify(ok=True)
