"""
Exemple de config. Copier en config.py et remplir les credentials.
"""

# ============================================================
# Pipeline Behavior
# ============================================================
INCREMENTAL_UPDATE = True

# ============================================================
# StatsBomb API - credentials from email
# ============================================================
STATSBOMB_CREDS = {
    "user": "your-statsbomb-email@example.com",
    "passwd": "your-statsbomb-password",
}

# ============================================================
# SkillCorner API - credentials from email
# ============================================================
SKILLCORNER_USERNAME = "your-skillcorner-email@example.com"
SKILLCORNER_PASSWORD = "your-skillcorner-password"

# ============================================================
# Target Competition (Ligue 1 - current season)
# ============================================================
STATSBOMB_COMPETITION_NAME = "Ligue 1"
STATSBOMB_COUNTRY = "France"

# ============================================================
# PostgreSQL
# ============================================================
POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "postgres",
    "user": "your_username",
    "password": "your_password",
}
DB_SCHEMA = "fc_metz"

# ============================================================
# Transfermarkt
# ============================================================
TRANSFERMARKT_BASE_URL = "https://www.transfermarkt.fr"
TRANSFERMARKT_LIGUE1_URL = "https://www.transfermarkt.fr/ligue-1/startseite/wettbewerb/FR1"
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}
TRANSFERMARKT_DELAY_SEC = 2.5
TRANSFERMARKT_DETAIL_DELAY_SEC = 2.5
