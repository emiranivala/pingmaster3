#crushe
import asyncio
import logging
import time
from pyromod import listen
from pyrogram import Client
from config import API_ID, API_HASH, BOT_TOKEN, STRING, MONGO_DB, SECONDS
from telethon.sync import TelegramClient
from motor.motor_asyncio import AsyncIOMotorClient

# Configure logging
loop = asyncio.get_event_loop()
logging.basicConfig(
    format="[%(levelname) 5s/%(asctime)s] %(name)s: %(message)s",
    level=logging.WARNING,
)

# Silence Pyrogram session logs
logging.getLogger("pyrogram").setLevel(logging.ERROR)
logging.getLogger("pyrogram.session").setLevel(logging.ERROR)
logging.getLogger("pyrogram.connection").setLevel(logging.ERROR)
# Silence empty message warnings
logging.getLogger("pyrogram.types.messages_and_media.message").setLevel(logging.ERROR)

app = Client(
    "RestrictBot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=10,
    no_updates=False,               # Ensure updates are received
    workdir="./"                    # Specify working directory for session files
)

botStartTime = time.time()
pro = Client(
    "ggbot", 
    api_id=API_ID, 
    api_hash=API_HASH, 
    session_string=STRING,
    no_updates=False,               # Ensure updates are received
    workdir="./"                    # Specify working directory for session files
)
sex = TelegramClient('sexrepo', API_ID, API_HASH).start(bot_token=BOT_TOKEN)


# MongoDB setup
tclient = AsyncIOMotorClient(MONGO_DB)
tdb = tclient["telegram_bot"]  # Your database
token = tdb["tokens"]  # Your tokens collection

async def create_ttl_index():
    """Ensure the TTL index exists for the `tokens` collection."""
    await token.create_index("expires_at", expireAfterSeconds=0)

# Run the TTL index creation when the bot starts
async def setup_database():
    await create_ttl_index()
    print("MongoDB TTL index created.")

# You can call this in your main bot file before starting the bot

async def restrict_bot():
    global BOT_ID, BOT_NAME, BOT_USERNAME
    await setup_database()
    await app.start()
    getme = await app.get_me()
    BOT_ID = getme.id
    BOT_USERNAME = getme.username
    if getme.last_name:
        BOT_NAME = getme.first_name + " " + getme.last_name
    else:
        BOT_NAME = getme.first_name
    if STRING:
        await pro.start()


loop.run_until_complete(restrict_bot())
