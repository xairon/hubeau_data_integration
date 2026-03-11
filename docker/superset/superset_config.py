import os

# Superset configuration
ROW_LIMIT = 5000
SUPERSET_WEBSERVER_PORT = 8088

# Secret key - SHOULD be overridden in production
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "CHANGE_ME_IN_PRODUCTION")

# Database connection
# Used for Superset's own metadata (users, dashboards, slices)
SQLALCHEMY_DATABASE_URI = os.environ.get("SUPERSET_SQLALCHEMY_DATABASE_URI")

# Flask-WTF flag for CSRF
WTF_CSRF_ENABLED = True
WTF_CSRF_EXEMPT_LIST = []
WTF_CSRF_TIME_LIMIT = 60 * 60 * 24 * 365

# Set this to False if you want to allow Public access to dashboards without login
PUBLIC_ROLE_LIKE = "Gamma"
AUTH_ROLE_PUBLIC = "Public"
AUTH_TYPE = 1  # 1 = Database Auth (standard)

# Feature flags
FEATURE_FLAGS = {
    "EMBEDDED_SUPERSET": True,
}
