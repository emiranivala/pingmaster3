
import asyncio
import importlib
import logging
from pyrogram import idle
from crushe.modules import ALL_MODULES
from aiojobs import create_scheduler
from crushe.core.mongo.plans_db import check_and_remove_expired_users
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from config import SECONDS

# Configure more detailed logging for error tracking
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s/%(asctime)s] %(name)s: %(message)s',
)

# Set specific loggers to higher levels to reduce noise
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("pyrogram.session").setLevel(logging.ERROR)
logging.getLogger("pyrogram.connection").setLevel(logging.ERROR)

loop = asyncio.get_event_loop()

# Set a higher timeout for asyncio operations
loop.slow_callback_duration = 1.0  # Default is 0.1 seconds

async def schedule_expiry_check():
    # This function now just runs the task without any loop or sleep
    await check_and_remove_expired_users()

async def crushe_boot():
    for all_module in ALL_MODULES:
        importlib.import_module("crushe.modules." + all_module)
    print("Bot deployed by Crushe...🎉")

    # Start the background task for checking expired users
    asyncio.create_task(schedule_expiry_check())
    # Keep the bot running
    await idle()
    print("Lol ...")

if __name__ == "__main__":
    loop.run_until_complete(crushe_boot())
