import os
from dotenv import load_dotenv

load_dotenv()

# Angel One API credentials
API_KEY = os.getenv("API_KEY")
CLIENT_ID = os.getenv("CLIENT_ID")
PASSWORD = os.getenv("PASSWORD")
TOTP_SECRET = os.getenv("TOTP_SECRET")

# Kafka settings
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "stock-ohlcv"

# MySQL settings
MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_PASS = os.getenv("MYSQL_PASS")
MYSQL_DB = "stock_pipeline"

# Stock settings
STOCK_TOKEN = "17939"
STOCK_SYMBOL = "HINDCOPPER"
ALERT_PRICE = 600.0