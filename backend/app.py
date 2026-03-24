"""
World Office Migrador ETL — Backend API v3.0
Flask + SQLite/PostgreSQL + PyJWT
"""
import os, sqlite3, secrets, datetime, re, threading, hmac, hashlib
from functools import wraps
from flask import Flask, Blueprint, request, jsonify, g, send_from_directory, current_app

try:
    import jwt as pyjwt
except ImportError:
    import PyJWT as pyjwt

# ── Database ─────────────────────────────────────────────────────
BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
DATABASE_URL = os.environ.get('DATABASE_URL', '')

if DATABASE_URL:
    DB_PATH = DATABASE_URL.split('@')[-1]
else:
    DATA_DIR = os.environ.get('RAILWAY_VOLUME_MOUNT_PATH', BASE_DIR)
    DB_PATH  = os.path.join(DATA_DIR, 'wo_migrador.db')

_db_local = threading.local()

class PGWrapper:
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

# ── Auth helpers ─────────────────────────────────────────────────
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
        if not header.startswith('Bearer '): return jsonify(error='Token requerido'), 401
        token = header[7:]
        try:
            payload = pyjwt.decode(token, current_app.config['SECRET_KEY'], algorithms=['HS256'])
        except pyjwt.ExpiredSignatureError: return jsonify(error='Sesión expirada'), 401
        except pyjwt.InvalidTokenError: return jsonify(error='Token inválido'), 401
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
        if not header.startswith('Bearer '): return jsonify(error='Token requerido'), 401
        token = header[7:]
        try:
            payload = pyjwt.decode(token, current_app.config['SECRET_KEY'], algorithms=['HS256'])
        except pyjwt.ExpiredSignatureError: return jsonify(error='Sesión expirada'), 401
        except pyjwt.InvalidTokenError: return jsonify(error='Token inválido'), 401
        db = get_db()
        ses = db.execute("SELECT * FROM sessions WHERE token_jti=? AND active=1", (payload['jti'],)).fetchone()
        if not ses: return jsonify(error='Sesión cerrada'), 401
        user = db.execute("SELECT * FROM users WHERE id=? AND active=1", (payload['sub'],)).fetchone()
        if not user: return jsonify(error='Usuario no encontrado'), 401
        g.user = dict(user); g.token_jti = payload['jti']
        if g.user['role'] != 'admin': return jsonify(error='Se requiere rol administrador'), 403
        return f(*args, **kwargs)
    return decorated

# ── Schema ───────────────────────────────────────────────────────
SCHEMA_SQLITE = """
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
    filename_out TEXT, orig_soft TEXT, dest_soft TEXT, module TEXT,
    records_in   INTEGER DEFAULT 0, records_out INTEGER DEFAULT 0,
    errors INTEGER DEFAULT 0, warnings INTEGER DEFAULT 0,
    duration_sec REAL, status TEXT DEFAULT 'completed',
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS app_config (key TEXT PRIMARY KEY, value TEXT NOT NULL);
"""

SCHEMA_PG = [
    """CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY, email TEXT NOT NULL UNIQUE,
        password_hash TEXT NOT NULL, name TEXT NOT NULL,
        initials TEXT NOT NULL DEFAULT 'US', phone TEXT,
        role TEXT NOT NULL DEFAULT 'user' CHECK(role IN ('admin','user')),
        active INTEGER NOT NULL DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, last_login TIMESTAMP)""",
    """CREATE TABLE IF NOT EXISTS sessions (
        id SERIAL PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        token_jti TEXT NOT NULL UNIQUE, active INTEGER NOT NULL DEFAULT 1,
        ip_address TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, expires_at TIMESTAMP NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS migration_logs (
        id SERIAL PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id),
        filename_out TEXT, orig_soft TEXT, dest_soft TEXT, module TEXT,
        records_in INTEGER DEFAULT 0, records_out INTEGER DEFAULT 0,
        errors INTEGER DEFAULT 0, warnings INTEGER DEFAULT 0,
        duration_sec REAL, status TEXT DEFAULT 'completed',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
    """CREATE TABLE IF NOT EXISTS app_config (key TEXT PRIMARY KEY, value TEXT NOT NULL)"""
]

SCHEMA = SCHEMA_PG if DATABASE_URL else SCHEMA_SQLITE

# ── Secret key ───────────────────────────────────────────────────
def _load_or_create_secret():
    key_file = os.path.join(BASE_DIR, 'secret.key')
    if os.path.exists(key_file):
        with open(key_file, 'r') as f:
            k = f.read().strip()
            if k: return k
    key = secrets.token_hex(32)
    with open(key_file, 'w') as f: f.write(key)
    return key

# ── Flask app ────────────────────────────────────────────────────
STATIC_DIR = os.path.join(BASE_DIR, '..', 'frontend')

app = Flask(__name__, static_folder=STATIC_DIR, static_url_path='')
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

@app.route('/<path:path>')
def static_files(path):
    try: return send_from_directory(STATIC_DIR, path)
    except: return send_from_directory(STATIC_DIR, 'index.html')

@app.route('/plantillas/<path:filename>')
def serve_plantilla(filename):
    return send_from_directory(os.path.join(STATIC_DIR,'plantillas'), filename, as_attachment=True)

# ── DB helpers ───────────────────────────────────────────────────
def make_jwt(user_id, email):
    jti     = secrets.token_hex(16)
    expires = datetime.datetime.utcnow() + datetime.timedelta(hours=app.config['JWT_EXPIRES_HOURS'])
    token   = pyjwt.encode({'sub':user_id,'email':email,'jti':jti,'exp':expires,
                             'iat':datetime.datetime.utcnow()},
                            app.config['SECRET_KEY'], algorithm='HS256')
    return token, jti, expires

# ── Auth routes ──────────────────────────────────────────────────
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
    user = dict(user)
    token, jti, expdt = make_jwt(user['id'], user['email'])
    db.execute("INSERT INTO sessions (user_id,token_jti,ip_address,expires_at) VALUES (?,?,?,?)",
               (user['id'], jti, request.remote_addr, expdt))
    db.execute("UPDATE users SET last_login=? WHERE id=?", (now, user['id']))
    db.commit()
    return jsonify(ok=True, token=token,
                   user=dict(id=user['id'],email=user['email'],name=user['name'],
                             initials=user['initials'],role=user['role']))

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
                   role=u['role'],phone=u.get('phone',''),last_login=str(u.get('last_login','')))

@app.route('/api/auth/change-password', methods=['POST'])
@require_auth
def change_password():
    d = request.get_json(force=True) or {}
    cur = d.get('current_password',''); nw = d.get('new_password','')
    if not cur or not nw: return jsonify(error='Contraseña actual y nueva requeridas'), 400
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE id=?", (g.user['id'],)).fetchone()
    if not verify_password(cur, dict(user)['password_hash']):
        return jsonify(error='Contraseña actual incorrecta'), 401
    db.execute("UPDATE users SET password_hash=? WHERE id=?", (hash_password(nw), g.user['id']))
    db.commit()
    return jsonify(ok=True)

# ── User routes ──────────────────────────────────────────────────
@app.route('/api/users', methods=['GET'])
@require_admin
def list_users():
    rows = get_db().execute("SELECT id,email,name,initials,phone,role,active,created_at,last_login FROM users ORDER BY id").fetchall()
    return jsonify([dict(r) for r in rows])

@app.route('/api/users', methods=['POST'])
@require_admin
def create_user():
    d = request.get_json(force=True) or {}
    email = (d.get('email') or '').strip().lower()
    password = d.get('password') or ''
    name  = (d.get('name') or '').strip()
    phone = (d.get('phone') or '').strip()
    role  = d.get('role', 'user')
    if not email or not password or not name: return jsonify(error='Email, contraseña y nombre son requeridos'), 400
    if not re.match(r'^[^@]+@[^@]+\.[^@]+$', email): return jsonify(error='Email inválido'), 400
    if role not in ('admin','user'): return jsonify(error='Rol inválido'), 400
    initials = ''.join(w[0].upper() for w in name.split()[:2])
    db = get_db()
    try:
        db.execute("INSERT INTO users (email,password_hash,name,initials,phone,role) VALUES (?,?,?,?,?,?)",
                   (email, hash_password(password), name, initials, phone, role))
        db.commit()
    except Exception as e:
        db.rollback()
        return jsonify(error='El correo ya está registrado'), 409
    user = db.execute("SELECT id,email,name,initials,phone,role,active,created_at FROM users WHERE email=?", (email,)).fetchone()
    return jsonify(dict(user)), 201

@app.route('/api/users/<int:uid>', methods=['GET'])
@require_auth
def get_user(uid):
    if g.user['role'] != 'admin' and g.user['id'] != uid: return jsonify(error='Sin permiso'), 403
    user = get_db().execute("SELECT id,email,name,initials,phone,role,active,created_at,last_login FROM users WHERE id=?", (uid,)).fetchone()
    if not user: return jsonify(error='Usuario no encontrado'), 404
    return jsonify(dict(user))

@app.route('/api/users/<int:uid>', methods=['PUT'])
@require_admin
def update_user(uid):
    d = request.get_json(force=True) or {}
    name = (d.get('name') or '').strip()
    if not name: return jsonify(error='Nombre requerido'), 400
    db = get_db()
    db.execute("UPDATE users SET name=?,phone=?,role=?,active=? WHERE id=?",
               (name, d.get('phone',''), d.get('role','user'), int(d.get('active',1)), uid))
    if d.get('password'):
        db.execute("UPDATE users SET password_hash=? WHERE id=?", (hash_password(d['password']), uid))
    db.commit()
    user = db.execute("SELECT id,email,name,initials,phone,role,active FROM users WHERE id=?", (uid,)).fetchone()
    return jsonify(dict(user))

@app.route('/api/users/<int:uid>', methods=['DELETE'])
@require_admin
def deactivate_user(uid):
    if uid == g.user['id']: return jsonify(error='No puedes desactivar tu propia cuenta'), 400
    get_db().execute("UPDATE users SET active=0 WHERE id=?", (uid,))
    get_db().commit()
    return jsonify(ok=True)

# ── Migration routes ─────────────────────────────────────────────
@app.route('/api/migrations', methods=['GET'])
@require_auth
def list_migrations():
    db = get_db()
    if g.user['role'] == 'admin':
        rows = db.execute("""SELECT ml.*,u.name user_name,u.email user_email
                             FROM migration_logs ml JOIN users u ON ml.user_id=u.id
                             ORDER BY ml.created_at DESC LIMIT 200""").fetchall()
    else:
        rows = db.execute("""SELECT ml.*,u.name user_name,u.email user_email
                             FROM migration_logs ml JOIN users u ON ml.user_id=u.id
                             WHERE ml.user_id=? ORDER BY ml.created_at DESC LIMIT 100""",
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
               (g.user['id'], d.get('filename_out',''), d.get('orig_soft',''), d.get('dest_soft',''),
                d.get('module',''), d.get('records_in',0), d.get('records_out',0),
                d.get('errors',0), d.get('warnings',0), d.get('duration_sec',0), d.get('status','completed')))
    db.commit()
    return jsonify(ok=True), 201

@app.route('/api/health', methods=['GET'])
def health():
    db = get_db()
    users = db.execute("SELECT COUNT(*) n FROM users WHERE active=1").fetchone()
    return jsonify(status='ok', version='3.0.0',
                   users=dict(users)['n'] if DATABASE_URL else users['n'],
                   timestamp=datetime.datetime.utcnow().isoformat())

# ── Init DB ──────────────────────────────────────────────────────
def init_db():
    with app.app_context():
        db = get_db()
        if os.environ.get('RESET_DB') == '1':
            for tbl in ['migration_logs','sessions','users','app_config']:
                try: db.execute(f"DROP TABLE IF EXISTS {tbl}" + (" CASCADE" if DATABASE_URL else ""))
                except: pass
            db.commit()
        stmts = SCHEMA if isinstance(SCHEMA, list) else [s.strip() for s in SCHEMA.strip().split(';') if s.strip()]
        for stmt in stmts:
            if stmt.strip():
                try:
                    db.execute(stmt)
                    db.commit()
                except Exception as e:
                    db.rollback()
                    if 'already exists' not in str(e).lower(): print(f"[DB] warning: {e}")
        for email, pwd, name, initials, phone, role in [
            ('jeffersonrojas@worldoffice.com.co','321','Jefferson Rojas','JR','3102666736','admin'),
            ('fabiobarahona@worldoffice.com.co','321','Fabio Barahona','FB','','user'),
            ('jorgerojas@worldoffice.com.co','321','Jorge Rojas','JO','','user'),
            ('samynaranjo@worldoffice.com.co','321','Samy Naranjo','SN','','user'),
        ]:
            if not db.execute("SELECT id FROM users WHERE email=?", (email,)).fetchone():
                db.execute("INSERT INTO users (email,password_hash,name,initials,phone,role) VALUES (?,?,?,?,?,?)",
                           (email, hash_password(pwd), name, initials, phone, role))
        db.commit()
        print(f"[DB] Lista OK — {DB_PATH}")

init_db()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5050))
    print(f"[WO Migrador] http://localhost:{port}")
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
