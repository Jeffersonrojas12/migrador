"""
World Office Migrador ETL — Backend API v3.0
Flask + SQLite + PyJWT + Gunicorn-ready
"""
import os, sqlite3, secrets, datetime, re, threading, hmac
from flask import Flask, request, jsonify, g, send_from_directory
from db import get_db, DB_PATH, BASE_DIR as _BASE_DIR
from auth_helpers import hash_password, verify_password, require_auth, require_admin
from routes_usuarios import usuarios_bp

try:
    import jwt as pyjwt
except ImportError:
    import PyJWT as pyjwt

BASE_DIR   = _BASE_DIR
STATIC_DIR = os.path.join(BASE_DIR, '..', 'frontend')


def _load_or_create_secret():
    key_file = os.path.join(BASE_DIR, 'secret.key')
    if os.path.exists(key_file):
        with open(key_file, 'r') as f:
            k = f.read().strip()
            if k: return k
    key = secrets.token_hex(32)
    with open(key_file, 'w') as f:
        f.write(key)
    return key

app = Flask(__name__, static_folder=STATIC_DIR, static_url_path='')
app.register_blueprint(usuarios_bp)
app.config.update(
    SECRET_KEY        = os.environ.get('WO_SECRET', _load_or_create_secret()),
    JWT_EXPIRES_HOURS = int(os.environ.get('JWT_HOURS', 8)),
)

@app.after_request
def cors(resp):
    resp.headers['Access-Control-Allow-Origin']  = '*'
    resp.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
    resp.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
    return resp

@app.route('/', defaults={'path': ''}, methods=['OPTIONS'])
@app.route('/<path:path>', methods=['OPTIONS'])
def preflight(path=''):
    return jsonify(ok=True), 200

@app.route('/')
def index():
    return send_from_directory(STATIC_DIR, 'index.html')

@app.route('/plantillas/<path:filename>')
def serve_plantilla(filename):
    return send_from_directory(os.path.join(STATIC_DIR,'plantillas'), filename, as_attachment=True)

# ── Thread-safe DB ────────────────────────────────────────────────
# get_db() -> imported from db.py

@app.teardown_appcontext
def close_db(e=None):
    pass  # Thread-local connections stay open for reuse

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    email         TEXT    NOT NULL UNIQUE COLLATE NOCASE,
    password_hash TEXT    NOT NULL,
    name          TEXT    NOT NULL,
    initials      TEXT    NOT NULL DEFAULT 'US',
    phone         TEXT,
    role          TEXT    NOT NULL DEFAULT 'user' CHECK(role IN ('admin','user')),
    active        INTEGER NOT NULL DEFAULT 1,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login    TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sessions (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_jti  TEXT    NOT NULL UNIQUE,
    active     INTEGER NOT NULL DEFAULT 1,
    ip_address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS migration_logs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id      INTEGER NOT NULL REFERENCES users(id),
    filename_out TEXT,
    orig_soft    TEXT,
    dest_soft    TEXT,
    module       TEXT,
    records_in   INTEGER DEFAULT 0,
    records_out  INTEGER DEFAULT 0,
    errors       INTEGER DEFAULT 0,
    warnings     INTEGER DEFAULT 0,
    duration_sec REAL,
    status       TEXT DEFAULT 'completed',
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS app_config (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

# hash_password() -> imported from auth_helpers.py

# verify_password() -> imported from auth_helpers.py

def make_jwt(user_id, email):
    jti     = secrets.token_hex(16)
    expires = datetime.datetime.utcnow() + datetime.timedelta(hours=app.config['JWT_EXPIRES_HOURS'])
    token   = pyjwt.encode({'sub':user_id,'email':email,'jti':jti,'exp':expires,
                             'iat':datetime.datetime.utcnow()},
                            app.config['SECRET_KEY'], algorithm='HS256')
    return token, jti, expires

def login():
    d = request.get_json(force=True) or {}
    email = (d.get('email') or '').strip().lower()
    password = d.get('password') or ''
    if not email or not password:
        return jsonify(error='Correo y contraseña son requeridos'), 400
    db = get_db()
    now = datetime.datetime.utcnow()
    user = db.execute("SELECT * FROM users WHERE email=? AND active=1", (email,)).fetchone()
    if not user or not verify_password(password, user['password_hash']):
        return jsonify(error='Correo o contraseña incorrectos'), 401
    token, jti, expdt = make_jwt(user['id'], user['email'])
    db.execute("INSERT INTO sessions (user_id,token_jti,ip_address,expires_at) VALUES (?,?,?,?)",
               (user['id'], jti, request.remote_addr, expdt))
    db.execute("UPDATE users SET last_login=? WHERE id=?", (now, user['id']))
    db.commit()
    return jsonify(ok=True, token=token,
                   user=dict(id=user['id'],email=user['email'],name=user['name'],
                             initials=user['initials'],role=user['role']))

@app.route('/api/auth/login', methods=['POST'])
def login():
    d = request.get_json(force=True) or {}
    email    = (d.get('email') or '').strip().lower()
    password = d.get('password') or ''
    if not email or not password:
        return jsonify(error='Correo y contraseña son requeridos'), 400
    db  = get_db()
    now = datetime.datetime.utcnow()
    user = db.execute("SELECT * FROM users WHERE email=? AND active=1", (email,)).fetchone()
    if not user or not verify_password(password, user['password_hash']):
        return jsonify(error='Correo o contraseña incorrectos'), 401
    token, jti, expdt = make_jwt(user['id'], user['email'])
    db.execute("INSERT INTO sessions (user_id,token_jti,ip_address,expires_at) VALUES (?,?,?,?)",
               (user['id'], jti, request.remote_addr, expdt))
    db.execute("UPDATE users SET last_login=? WHERE id=?", (now, user['id']))
    db.commit()
    return jsonify(ok=True, token=token,
                   user=dict(id=user['id'], email=user['email'], name=user['name'],
                             initials=user['initials'], role=user['role']))

@app.route('/api/auth/logout', methods=['POST'])
@require_auth
def logout():
    get_db().execute("UPDATE sessions SET active=0 WHERE token_jti=?", (g.token_jti,))
    get_db().commit()
    return jsonify(ok=True)

@app.route('/api/auth/me', methods=['GET'])
@require_auth
def me():
    u = g.user
    return jsonify(id=u['id'],email=u['email'],name=u['name'],initials=u['initials'],
                   phone=u['phone'],role=u['role'],last_login=u['last_login'])

@app.route('/api/auth/change-password', methods=['POST'])
@require_auth
def change_password():
    d = request.get_json(force=True) or {}
    current = d.get('current_password','')
    new_pw  = d.get('new_password','')
    if not current or not new_pw:
        return jsonify(error='Contraseña actual y nueva son requeridas'), 400
    if len(new_pw) < 2:
        return jsonify(error='La nueva contraseña es muy corta'), 400
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE id=?", (g.user['id'],)).fetchone()
    if not verify_password(current, user['password_hash']):
        return jsonify(error='Contraseña actual incorrecta'), 401
    db.execute("UPDATE users SET password_hash=? WHERE id=?", (hash_password(new_pw), g.user['id']))
    db.commit()
    return jsonify(ok=True, message='Contraseña actualizada correctamente')

# ── Users CRUD ────────────────────────────────────────────────────
# /api/users routes -> routes_usuarios.py (Blueprint)

@app.route('/api/migrations', methods=['GET'])
@require_auth
def list_migrations():
    db = get_db()
    if g.user['role'] == 'admin':
        rows = db.execute("""SELECT ml.*,u.name user_name,u.email user_email
               FROM migration_logs ml JOIN users u ON ml.user_id=u.id
               ORDER BY ml.created_at DESC LIMIT 100""").fetchall()
    else:
        rows = db.execute("SELECT * FROM migration_logs WHERE user_id=? ORDER BY created_at DESC LIMIT 50",
                          (g.user['id'],)).fetchall()
    return jsonify([dict(r) for r in rows])

@app.route('/api/migrations', methods=['POST'])
@require_auth
def log_migration():
    d = request.get_json(force=True) or {}
    db = get_db()
    db.execute("""INSERT INTO migration_logs
       (user_id,filename_out,orig_soft,dest_soft,module,records_in,records_out,errors,warnings,duration_sec,status)
       VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        (g.user['id'],d.get('filename_out'),d.get('orig_soft'),d.get('dest_soft'),d.get('module'),
         d.get('records_in',0),d.get('records_out',0),d.get('errors',0),d.get('warnings',0),
         d.get('duration_sec'),d.get('status','completed')))
    db.commit()
    return jsonify(ok=True), 201

@app.route('/api/health', methods=['GET'])
def health():
    db = get_db()
    users = db.execute("SELECT COUNT(*) n FROM users WHERE active=1").fetchone()['n']
    migs  = db.execute("SELECT COUNT(*) n FROM migration_logs").fetchone()['n']
    return jsonify(status='ok',version='3.0.0',users=users,migrations=migs,
                   timestamp=datetime.datetime.utcnow().isoformat())

def init_db():
    with app.app_context():
        db = get_db()
        db.executescript(SCHEMA)
  # column already exists
        for email, pwd, name, initials, phone, role in [
            ('jeffersonrojas@worldoffice.com.co','2','Jefferson Rojas','JR','3102666736','admin'),
            ('fabiobarahona@worldoffice.com.co','3','Fabio Barahona','FB','','user'),
            ('jorgerojas@worldoffice.com.co','3','Jorge Rojas','JO','','user'),
            ('samynaranjo@worldoffice.com.co','4','samy Naranjo','SV','','user'),
        ]:
            if not db.execute("SELECT id FROM users WHERE email=?", (email,)).fetchone():
                db.execute("INSERT INTO users (email,password_hash,name,initials,phone,role) VALUES (?,?,?,?,?,?)",
                           (email, hash_password(pwd), name, initials, phone, role))
        db.commit()
        print(f"[DB] Lista → {DB_PATH}")

if __name__ == '__main__':
    init_db()
    port = int(os.environ.get('PORT', 5050))
    print(f"[WO Migrador v3] http://localhost:{port}")
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
