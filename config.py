from os import getenv

API_ID = int(getenv("API_ID", "27134561"))
API_HASH = getenv("API_HASH", "fa3c15f5ed4e3226ce9a929e4b9b2806")
BOT_TOKEN = getenv("BOT_TOKEN", "")
OWNER_ID = list(map(int, getenv("OWNER_ID", "922270982").split()))
MONGO_DB = getenv("MONGO_DB", "")
LOG_GROUP = getenv("LOG_GROUP", "-1002293309406")
CHANNEL_ID = int(getenv("CHANNEL_ID", "-1002433933366"))
FREEMIUM_LIMIT = int(getenv("FREEMIUM_LIMIT", "20"))
PREMIUM_LIMIT = int(getenv("PREMIUM_LIMIT", "500000000000000000"))
WEBSITE_URL = getenv("WEBSITE_URL", None)
AD_API = getenv("AD_API", None)
STRING = getenv("STRING", None)
YT_COOKIES = getenv("YT_COOKIES", None)
INSTA_COOKIES = getenv("INSTA_COOKIES", None)
# FloodWait handling settings
# Set to a reasonable value that balances between retrying too quickly (causing more FloodWait)
# and waiting too long (causing user experience issues)
SECONDS = 180  # 3-minute delay for FloodWait threshold
