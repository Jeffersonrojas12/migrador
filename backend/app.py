"""
World Office Migrador ETL — Backend API v3.0
Flask + SQLite + PyJWT + Email OTP + Gunicorn-ready
"""
import os, sqlite3, secrets, datetime, re, smtplib, threading, hmac
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
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

SMTP_HOST = 'smtp.gmail.com'
SMTP_PORT = 587
SMTP_USER = 'jeffersonrojas@worldoffice.com.co'
SMTP_PASS = 'kixezrztdclmdovy'

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
    OTP_EXPIRES_MIN   = int(os.environ.get('OTP_MINUTES', 5)),
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
CREATE TABLE IF NOT EXISTS otp_codes (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    code       TEXT    NOT NULL,
    channel    TEXT    NOT NULL DEFAULT 'email',
    used       INTEGER NOT NULL DEFAULT 0,
    expires_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

def send_otp_email(dest_email, dest_name, code):
    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = f'🔐 Tu código de acceso World Office: {code}'
        msg['From']    = f'World Office Migrador <{SMTP_USER}>'
        msg['To']      = dest_email
        html_body = f"""<div style="font-family:Arial,sans-serif;max-width:480px;margin:0 auto;background:#06090f;color:#e4eeff;border-radius:16px;overflow:hidden">
          <div style="background:linear-gradient(135deg,#00d2ff,#0044ff);padding:28px;text-align:center">
            <div style="font-size:32px;font-weight:900;letter-spacing:.05em">World Office</div>
            <div style="font-size:13px;opacity:.85;margin-top:4px">Migrador ETL — Verificación de acceso</div>
          </div>
          <div style="padding:32px">
            <p style="margin:0 0 8px;color:#7a9bc8;font-size:14px">Hola, <b style="color:#e4eeff">{dest_name}</b></p>
            <p style="margin:0 0 24px;color:#7a9bc8;font-size:14px;line-height:1.6">Código válido por {app.config['OTP_EXPIRES_MIN']} minutos.</p>
            <div style="background:#0f162b;border:2px solid rgba(0,210,255,.3);border-radius:12px;padding:24px;text-align:center;margin-bottom:24px">
              <div style="font-size:11px;color:#7a9bc8;font-family:monospace;text-transform:uppercase;letter-spacing:.15em;margin-bottom:8px">Código de verificación</div>
              <div style="font-size:42px;font-weight:900;letter-spacing:.3em;color:#00d2ff;font-family:monospace">{code}</div>
            </div>
            <p style="margin:0;color:#3d5a80;font-size:12px">No compartas este código con nadie.</p>
          </div>
          <div style="background:#0c1120;padding:16px;text-align:center;font-size:11px;color:#3d5a80">World Office &copy; {datetime.datetime.now().year}</div>
        </div>"""
        text_body = f"Hola {dest_name},\n\nTu código: {code}\nVálido por {app.config['OTP_EXPIRES_MIN']} minutos."
        msg.attach(MIMEText(text_body, 'plain', 'utf-8'))
        msg.attach(MIMEText(html_body, 'html', 'utf-8'))
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(SMTP_USER, dest_email, msg.as_string())
        return True
    except Exception as e:
        print(f"[EMAIL ERROR] {e}")
        return False

# require_auth, require_admin -> imported from auth_helpers.py

# ── Auth ──────────────────────────────────────────────────────────
@app.route('/api/auth/login', methods=['POST'])
def login():
    d = request.get_json(force=True) or {}
    email = (d.get('email') or '').strip().lower()
    password = d.get('password') or ''
    if not email or not password:
        return jsonify(error='Correo y contraseña son requeridos'), 400
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE email=? AND active=1", (email,)).fetchone()
    if not user or not verify_password(password, user['password_hash']):
        return jsonify(error='Correo o contraseña incorrectos'), 401
    db.execute("UPDATE otp_codes SET used=1 WHERE user_id=? AND used=0", (user['id'],))
    code    = str(secrets.randbelow(900000) + 100000)
    expires = datetime.datetime.utcnow() + datetime.timedelta(minutes=app.config['OTP_EXPIRES_MIN'])
    db.execute("INSERT INTO otp_codes (user_id,code,channel,expires_at) VALUES (?,?,?,?)",
               (user['id'], code, 'email', expires))
    db.commit()
    sent = send_otp_email(user['email'], user['name'], code)
    parts = user['email'].split('@')
    masked = parts[0][0] + '*'*(len(parts[0])-1) + '@' + parts[1]
    phone = user['phone'] or ''
    return jsonify(ok=True, user_id=user['id'], name=user['name'], initials=user['initials'],
                   masked_email=masked, masked_phone='***'+phone[-4:] if len(phone)>=4 else '***',
                   email_sent=sent, expires_min=app.config['OTP_EXPIRES_MIN'])

@app.route('/api/auth/otp/verify', methods=['POST'])
def otp_verify():
    d = request.get_json(force=True) or {}
    user_id = d.get('user_id'); code = str(d.get('code') or '').strip()
    if not user_id or not code: return jsonify(error='Parámetros inválidos'), 400
    db = get_db(); now = datetime.datetime.utcnow()
    otp = db.execute("SELECT * FROM otp_codes WHERE user_id=? AND used=0 AND expires_at>? ORDER BY created_at DESC LIMIT 1",
                     (user_id, now)).fetchone()
    if not otp: return jsonify(error='Código expirado. Inicia sesión nuevamente.'), 401
    if not hmac.compare_digest(otp['code'], code): return jsonify(error='Código incorrecto.'), 401
    db.execute("UPDATE otp_codes SET used=1 WHERE id=?", (otp['id'],))
    user = db.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
    token, jti, expdt = make_jwt(user['id'], user['email'])
    db.execute("INSERT INTO sessions (user_id,token_jti,ip_address,expires_at) VALUES (?,?,?,?)",
               (user['id'], jti, request.remote_addr, expdt))
    db.execute("UPDATE users SET last_login=? WHERE id=?", (now, user['id']))
    db.commit()
    return jsonify(ok=True, token=token,
                   user=dict(id=user['id'],email=user['email'],name=user['name'],
                             initials=user['initials'],role=user['role']))

@app.route('/api/auth/resend', methods=['POST'])
def otp_resend():
    d = request.get_json(force=True) or {}
    user_id = d.get('user_id')
    if not user_id: return jsonify(error='user_id requerido'), 400
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE id=? AND active=1", (user_id,)).fetchone()
    if not user: return jsonify(error='Usuario no encontrado'), 404
    db.execute("UPDATE otp_codes SET used=1 WHERE user_id=? AND used=0", (user_id,))
    code = str(secrets.randbelow(900000) + 100000)
    expires = datetime.datetime.utcnow() + datetime.timedelta(minutes=app.config['OTP_EXPIRES_MIN'])
    db.execute("INSERT INTO otp_codes (user_id,code,channel,expires_at) VALUES (?,?,?,?)",
               (user_id, code, 'email', expires))
    db.commit()
    sent = send_otp_email(user['email'], user['name'], code)
    return jsonify(ok=True, email_sent=sent, expires_min=app.config['OTP_EXPIRES_MIN'])

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
                   smtp=SMTP_USER,timestamp=datetime.datetime.utcnow().isoformat())

def init_db():
    with app.app_context():
        db = get_db()
        db.executescript(SCHEMA)
        for email, pwd, name, initials, phone, role in [
            ('jeffersonrojas@worldoffice.com.co','2','Jefferson Rojas','JR','3102666736','admin'),
            ('fabiobarahona@worldoffice.com.co','3','Fabio Barahona','FB','','user'),
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
