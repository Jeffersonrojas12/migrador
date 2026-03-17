#!/usr/bin/env python3
"""
World Office Migrador ETL — Servidor de inicio
Ejecuta backend + sirve frontend desde un solo proceso.
"""
import os, sys

# Agregar el directorio backend al path
BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(BASE, 'backend'))

import app as flask_app

if __name__ == '__main__':
    flask_app.init_db()
    port = int(os.environ.get('PORT', 5050))
    import socket
    try:
        hostname = socket.gethostname()
        local_ip = socket.gethostbyname(hostname)
    except:
        local_ip = '(no disponible)'
    print("=" * 55)
    print("  World Office Migrador ETL — Backend + Frontend")
    print("=" * 55)
    print(f"  URL local: http://localhost:{port}")
    print(f"  URL red:   http://{local_ip}:{port}")
    print(f"  API:       http://localhost:{port}/api")
    print(f"  Health:    http://localhost:{port}/api/health")
    print(f"  DB:        {flask_app.DB_PATH}")
    print("=" * 55)
    print("  Usuario:   jeffersonrojas@worldoffice.com.co")
    print("  Clave:     2")
    print("=" * 55)
    flask_app.app.run(host='0.0.0.0', port=port, debug=False)
