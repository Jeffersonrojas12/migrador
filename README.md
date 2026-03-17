# World Office — Migrador ETL v2.1

Sistema de migración de terceros Siigo Nube → World Office Cloud.
Backend Flask + SQLite + JWT. Frontend HTML/JS con SheetJS.

---

## Requisitos

- Python 3.10+
- Pip

---

## Instalación

```bash
# 1. Instalar dependencias
pip install -r backend/requirements.txt

# 2. Iniciar servidor (backend + frontend juntos)
python start.py
```

El servidor queda disponible en: **http://localhost:5050**

---

## Credenciales iniciales

| Campo    | Valor                               |
|----------|-------------------------------------|
| Correo   | jeffersonrojas@worldoffice.com.co   |
| Clave    | 2                                   |
| Teléfono | 3102666736                          |
| Rol      | admin                               |

---

## Flujo de autenticación (3 pasos)

1. **Login** → ingresa correo + contraseña
2. **Canal OTP** → elige SMS o Correo
3. **Verificación** → ingresa el código de 6 dígitos

> En modo demo el código aparece en pantalla (campo amarillo).
> En producción integra Twilio (SMS) o smtplib (Email).

---

## Estructura del proyecto

```
wo_migrador/
├── start.py                  ← Punto de entrada (ejecutar esto)
├── README.md
├── backend/
│   ├── app.py                ← API Flask completa
│   ├── requirements.txt
│   └── wo_migrador.db        ← SQLite (se crea automáticamente)
└── frontend/
    └── index.html            ← App web completa
```

---

## API Endpoints

### Auth
| Método | Ruta                    | Descripción             |
|--------|-------------------------|-------------------------|
| POST   | /api/auth/login         | Validar credenciales    |
| POST   | /api/auth/otp/send      | Generar y enviar OTP    |
| POST   | /api/auth/otp/verify    | Verificar OTP → JWT     |
| POST   | /api/auth/logout        | Cerrar sesión           |
| GET    | /api/auth/me            | Perfil del usuario      |

### Usuarios (solo admin)
| Método | Ruta                    | Descripción             |
|--------|-------------------------|-------------------------|
| GET    | /api/users              | Listar usuarios         |
| POST   | /api/users              | Crear usuario           |
| PUT    | /api/users/:id          | Editar usuario          |
| DELETE | /api/users/:id          | Desactivar usuario      |

### Migraciones
| Método | Ruta                    | Descripción             |
|--------|-------------------------|-------------------------|
| GET    | /api/migrations         | Historial               |
| POST   | /api/migrations         | Guardar log             |

### Sistema
| Método | Ruta                    | Descripción             |
|--------|-------------------------|-------------------------|
| GET    | /api/health             | Estado del servidor     |

---

## Variables de entorno (opcional)

| Variable      | Default         | Descripción                     |
|---------------|-----------------|---------------------------------|
| PORT          | 5050            | Puerto del servidor             |
| WO_SECRET     | auto-generated  | Clave secreta JWT               |
| JWT_HOURS     | 8               | Duración de sesión (horas)      |
| OTP_MINUTES   | 5               | Duración del código OTP (min)   |
| DEBUG         | false           | Modo debug Flask                |

---

## Seguridad

- Contraseñas con PBKDF2-HMAC-SHA256 + salt de 16 bytes
- Tokens JWT firmados con HS256
- Cada sesión registrada con JTI único en BD
- OTPs de un solo uso con expiración de 5 minutos
- Comparación de códigos con `hmac.compare_digest` (anti timing-attack)
