import os
from dotenv import load_dotenv
from urllib.parse import quote_plus
from sqlalchemy import create_engine




load_dotenv()

# Local Ollama API (for mistral_local)
MISTRAL_API_URL  = "http://localhost:11434/api/generate"

# API Keys
GEMINI_API_KEY = "AIzaSyC8vJObb0iCn0fAHXxK7DYxJaqj-DD7Bxo"
MISTRAL_API_KEY = "IotlgX9OC7gWRj0WqHuT5xdhT1LNkNne"
MISTRAL_MODEL="mistral-small-latest"

# Choose between: "gemini", "mistral_cloud", "mistral_local"
ACTIVE_LLM = "mistral_cloud"
MODEL_NAME = "gemini-2.5-flash" 


# ============ Gemini Configuration ============
MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY", "IotlgX9OC7gWRj0WqHuT5xdhT1LNkNne")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "AIzaSyC8vJObb0iCn0fAHXxK7DYxJaqj-DD7Bxo")
MODEL_NAME = os.getenv("MODEL_NAME", "gemini-2.5-flash")

# ============ Folder Configuration ============
GRAPH_FOLDER = os.getenv("GRAPH_FOLDER", "graphs")
UPLOAD_FOLDER = os.getenv("UPLOAD_FOLDER", "uploads")
TEMP_UPLOAD_FOLDER = os.getenv("TEMP_UPLOAD_FOLDER", "uploads")

# ============ MySQL Configuration(lenovo server) ============
# MYSQL_CONFIG = {
#     "host": os.getenv("MYSQL_HOST", "116.193.134.6"),
#     "port": int(os.getenv("MYSQL_PORT", "3306")),
#     "user": os.getenv("MYSQL_USER", "lmysqluser"),
#     "password": os.getenv("MYSQL_PASSWORD", "lenovo@429"),
#     # "database": os.getenv("MYSQL_DATABASE", "NEW_DPT_V2")
#     "database": os.getenv("MYSQL_DATABASE", "ai_soulbuddy")
# }
# ============ MySQL Configuration(vps server) ============
MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "157.173.221.226"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": os.getenv("MYSQL_USER", "aiinhome_2"),
    "password": os.getenv("MYSQL_PASSWORD", "Aiin@2026"),
    "database": os.getenv("MYSQL_DATABASE", "TraverseAi_dis")
}

# --- SQLAlchemy Engine ---
MYSQL_URI = (
    f"mysql+pymysql://{MYSQL_CONFIG['user']}:{quote_plus(MYSQL_CONFIG['password'])}"
    f"@{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}"
)

engine = create_engine(MYSQL_URI, pool_recycle=3600, pool_pre_ping=True)


# ============ Base URL ============
BASE_URL = os.getenv("BASE_URL", "http://122.163.121.176:3004")

# ============ Misc Settings ============
MAX_SAMPLE_VALUES = int(os.getenv("MAX_SAMPLE_VALUES", "100"))



ARANGO_HOST = os.getenv("ARANGO_HOST", "https://1a87076ffc68.arangodb.cloud:8529")
ARANGO_USER = os.getenv("ARANGO_USER", "root")
ARANGO_PASS = os.getenv("ARANGO_PASS", "acaZXdQkU9SuRNsWoUXa")
ARANGO_DB = os.getenv("ARANGO_DB", "graph_ai")



