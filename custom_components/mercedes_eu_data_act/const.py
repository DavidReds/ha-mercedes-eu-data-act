"""Constants for the Mercedes-Benz EU Data Act integration."""

DOMAIN = "mercedes_eu_data_act"

# Configuration keys
CONF_CLIENT_ID = "client_id"
CONF_CLIENT_SECRET = "client_secret"
CONF_TOKEN_URL = "token_url"
CONF_KAFKA_BOOTSTRAP = "kafka_bootstrap"
CONF_KAFKA_TOPIC = "kafka_topic"
CONF_CONSUMER_GROUP = "consumer_group"
CONF_OAUTH_SCOPE = "oauth_scope"
CONF_API_BASE_URL = "api_base_url"

# Default Mercedes-Benz EU Data Act Endpoints (from MB Developer Portal Docs)
DEFAULT_TOKEN_URL = "https://ssoalpha.dvb.corpinter.net/v1/token"
DEFAULT_KAFKA_BOOTSTRAP = "bootstrap.streaming.connect-business.net:443"
DEFAULT_API_BASE_URL = "https://api.connect-business.net/v1"
DEFAULT_OAUTH_SCOPE = "openid"

# EU Data Act Packages (6001 - 6009)
DATA_PACKAGES = {
    "6001": "Body Features Data",
    "6002": "Climate and Comfort Data",
    "6003": "Energy and Consumption Data",
    "6004": "Maintenance and Diagnostic Data",
    "6005": "Navigation and Positioning Data",
    "6006": "Vehicle Operation Data",
    "6007": "Infotainment and User Interactions Data",
    "6009": "Wallbox Specific Data",
}

# Dispatcher signal for real-time Kafka push updates
SIGNAL_NEW_TELEMETRY = f"{DOMAIN}_new_telemetry"
