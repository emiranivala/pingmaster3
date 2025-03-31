import time
import random
import string
import asyncio
from pyrogram import filters, Client
from crushe import app
from config import API_ID, API_HASH, FREEMIUM_LIMIT, PREMIUM_LIMIT, OWNER_ID, SECONDS
from crushe.core.get_func import get_msg
from crushe.core.func import *
from crushe.core.mongo import db
from crushe.modules.shrink import is_user_verified
from pyrogram.errors import FloodWait, UserNotParticipant
from requests.exceptions import ConnectionError, Timeout, RequestException
from datetime import datetime, timedelta
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
# Import error handling utilities
from crushe.core.error_handler import safe_execute, retry_with_backoff, exponential_backoff

async def generate_random_name(length=8):
    return ''.join(random.choices(string.ascii_lowercase, k=length))


users_loop = {}
interval_set = {}
batch_mode = {}

async def process_and_upload_link(userbot, user_id, msg_id, link, retry_count, message):
    try:
        # Use safe_execute to handle FloodWait and other errors
        await safe_execute(get_msg, userbot, user_id, msg_id, link, retry_count, message)
        # Increased sleep time to avoid hitting rate limits
        await asyncio.sleep(5.0)
    finally:
        # Auto-delete the "Processing..." message regardless of outcome.
        try:
            await app.delete_messages(message.chat.id, msg_id)
        except Exception:
            pass


async def check_interval(user_id, freecheck):
    if freecheck != 1 or await is_user_verified(user_id):  
        return True, None

    now = datetime.now()

    if user_id in interval_set:
        cooldown_end = interval_set[user_id]
        if now < cooldown_end:
            remaining_time = (cooldown_end - now).seconds // 60
            return False, f"Please wait {remaining_time} minute(s) before sending another link. Alternatively, purchase premium for instant access.\n\n> Hey 👋 You can use /token to use the bot free for 3 hours without any time limit."
        else:
            del interval_set[user_id]  

    return True, None

async def set_interval(user_id, interval_minutes=5):
    now = datetime.now()
    interval_set[user_id] = now + timedelta(minutes=interval_minutes)


@app.on_message(filters.regex(r'https?://(?:www\.)?t\.me/[^\s]+') & filters.private)
@retry_with_backoff(max_retries=5, initial_delay=2.0, max_delay=SECONDS)
async def single_link(_, message):
    user_id = message.chat.id
    if user_id in batch_mode:
        return

    if users_loop.get(user_id, False):
        await message.reply(
            "You already have an ongoing process. Please wait for it to finish or cancel it with /cancel."
        )
        return    

    freecheck = await chk_user(message, user_id)
    if freecheck == 1 and FREEMIUM_LIMIT == 0 and user_id not in OWNER_ID:
        await message.reply("Freemium service is currently not available. Upgrade to premium for access.")
        return

    can_proceed, response_message = await check_interval(user_id, freecheck)
    if not can_proceed:
        await message.reply(response_message)
        return

    users_loop[user_id] = True
    link = get_link(message.text) 
    userbot = None
    msg = None
    try:
        join = await subscribe(_, message)
        if join == 1:
            users_loop[user_id] = False
            return

        msg = await message.reply("Processing...")

        if 't.me/' in link and 't.me/+' not in link and 't.me/c/' not in link and 't.me/b/' not in link:
            data = await db.get_data(user_id)
            if data and data.get("session"):
                session = data.get("session")
                try:
                    device = 'Vivo Y20'
                    userbot = Client(
                        ":userbot:",
                        api_id=API_ID,
                        api_hash=API_HASH,
                        device_model=device,
                        session_string=session,
                        # Add connection settings to improve reliability
                        flood_sleep_threshold=SECONDS,  # Sleep on FloodWait up to SECONDS
                        retry_delay=2,             # Initial retry delay
                        max_retries=5,             # Maximum number of retries
                        no_updates=True,           # Disable updates to reduce overhead
                        workers=4                  # Limit number of workers to prevent overloading
                    )
                    await userbot.start()
                except Exception as e:
                    userbot = None
            else:
                userbot = None

            # Use safe_execute to handle FloodWait and other errors
            await safe_execute(process_and_upload_link, userbot, user_id, msg.id, link, 0, message)
            await set_interval(user_id, interval_minutes=5)
            users_loop[user_id] = False
            return

        data = await db.get_data(user_id)

        if data and data.get("session"):
            session = data.get("session")
            try:
                device = 'Vivo Y20'
                userbot = Client(
                    ":userbot:", 
                    api_id=API_ID, 
                    api_hash=API_HASH, 
                    device_model=device, 
                    session_string=session,
                    # Add connection settings to improve reliability
                    flood_sleep_threshold=60,  # Sleep on FloodWait up to 60 seconds
                    retry_delay=1,             # Initial retry delay
                    max_retries=5              # Maximum number of retries
                )
                await userbot.start()                
            except Exception as e:
                users_loop[user_id] = False
                return await msg.edit_text(f"Login expired /login again... Error: {str(e)}")
        else:
            users_loop[user_id] = False
            await msg.edit_text("Login in bot first ...")
            return

        try:
            if 't.me/+' in link:
                # Use retry_with_backoff for userbot_join
                @retry_with_backoff(max_retries=3)
                async def join_with_retry():
                    return await userbot_join(userbot, link)
                
                q = await join_with_retry()
                await msg.edit_text(q)
            elif 't.me/c/' in link:
                # Use safe_execute to handle FloodWait and other errors
                await safe_execute(process_and_upload_link, userbot, user_id, msg.id, link, 0, message)
                await set_interval(user_id, interval_minutes=5)
            else:
                await msg.edit_text("Invalid link format.")
        except Exception as e:
            await msg.edit_text(f"Link: `{link}`\n\n**Error:** {str(e)}")

    except FloodWait as fw:
        wait_time = fw.value if hasattr(fw, 'value') else fw.x
        if msg:
            await msg.edit_text(f'Try again after {wait_time} seconds due to floodwait from telegram.')
        # Sleep for the required time and then retry automatically
        await asyncio.sleep(wait_time + 1)
        if msg:
            await msg.edit_text("Retrying after FloodWait...")
            # Retry the operation after waiting
            try:
                if 't.me/c/' in link:
                    await safe_execute(process_and_upload_link, userbot, user_id, msg.id, link, 0, message)
            except Exception as e:
                await msg.edit_text(f"Retry failed: {str(e)}")

    except ConnectionError as e:
        if msg:
            await msg.edit_text(f"Connection error: {str(e)}. Please try again later.")
    except Timeout as e:
        if msg:
            await msg.edit_text(f"Request timed out: {str(e)}. Please try again later.")
    except RequestException as e:
        if msg:
            await msg.edit_text(f"Network error: {str(e)}. Please try again later.")
    except UserNotParticipant:
        if msg:
            await msg.edit_text("You need to join the required channel first.")
    except Exception as e:
        if msg:
            await msg.edit_text(f"Link: `{link}`\n\n**Error:** {str(e)}")
    finally:
        if userbot and userbot.is_connected:  
            await userbot.stop()
        users_loop[user_id] = False  




@app.on_message(filters.command("batch") & filters.private)
@retry_with_backoff(max_retries=5, initial_delay=2.0, max_delay=SECONDS)
async def batch_link(_, message):
    user_id = message.chat.id

    if users_loop.get(user_id, False):  
        await app.send_message(
            message.chat.id,
            "You already have a batch process running. Please wait for it to complete before starting a new one."
        )
        return

    freecheck = await chk_user(message, user_id)
    if freecheck == 1 and FREEMIUM_LIMIT == 0 and user_id not in OWNER_ID:
        await message.reply("Freemium service is currently not available. Upgrade to premium for access.")
        return

    toker = await is_user_verified(user_id)
    if toker:
        max_batch_size = (FREEMIUM_LIMIT + 1)
        freecheck = 0  
    else:
        freecheck = await chk_user(message, user_id)
        if freecheck == 1:
            max_batch_size = FREEMIUM_LIMIT  
        else:
            max_batch_size = PREMIUM_LIMIT

    attempts = 0
    while attempts < 3:
        start = await app.ask(message.chat.id, text="Please send the start link.")
        start_id = start.text.strip()
        s = start_id.split("/")[-1]  
        try:
            cs = int(s)  
            break  
        except ValueError:
            attempts += 1
            if attempts == 3:
                await app.send_message(message.chat.id, "You have exceeded the maximum number of attempts. Please try again later.")
                return
            await app.send_message(message.chat.id, "Invalid link. Please send again ...")

    attempts = 0
    while attempts < 3:
        num_messages = await app.ask(message.chat.id, text="How many messages do you want to process?")
        try:
            cl = int(num_messages.text.strip())  
            if cl <= 0 or cl > max_batch_size:
                raise ValueError(f"Number of messages must be between 1 and {max_batch_size}.")
            break  
        except ValueError as e:
            attempts += 1
            if attempts == 3:
                await app.send_message(message.chat.id, "You have exceeded the maximum number of attempts. Please try again later.")
                return
            await app.send_message(message.chat.id, f"Invalid number: {e}. Please enter a valid number again ...")

    can_proceed, response_message = await check_interval(user_id, freecheck)
    if not can_proceed:
        await message.reply(response_message)
        return

    join_button = InlineKeyboardButton("Join Channel", url="https://t.me/+3bMBj190KOc3YzNk")
    keyboard = InlineKeyboardMarkup([[join_button]])

    pin_msg = await app.send_message(
        user_id,
        "⚡\n__Processing: 0/{cl}__\n\nBatch process started",
        reply_markup=keyboard
    )
    try:
        await pin_msg.pin()
    except Exception as e:
        await pin_msg.pin(both_sides=True)
    
    users_loop[user_id] = True
    try:
        for i in range(cs, cs + cl):
            if user_id in users_loop and users_loop[user_id]:
                try:
                    x = start_id.split('/')
                    y = x[:-1]
                    result = '/'.join(y)
                    url = f"{result}/{i}"
                    link = get_link(url)
                    
                    if 't.me/' in link and 't.me/b/' not in link and 't.me/c' not in link:
                        userbot = None
                        data = await db.get_data(user_id)
                        if data and data.get("session"):
                            session = data.get("session")
                            try:
                                device = 'Vivo Y20'
                                userbot = Client(
                                    ":userbot:",
                                    api_id=API_ID,
                                    api_hash=API_HASH,
                                    device_model=device,
                                    session_string=session,
                                    # Add connection settings to improve reliability
                                    flood_sleep_threshold=SECONDS,  # Sleep on FloodWait up to SECONDS
                                    retry_delay=1,             # Initial retry delay
                                    max_retries=5              # Maximum number of retries
                                )
                                await userbot.start()
                            except Exception as e:
                                userbot = None
                        else:
                            userbot = None
                        msg = await app.send_message(message.chat.id, f"Processing Crushe...")
                        await safe_execute(process_and_upload_link, userbot, user_id, msg.id, link, 0, message)
                        await pin_msg.edit_text(
                        f"⚡\n__Processing: {i - cs + 1}/{cl}__\n\nBatch process started",
                        reply_markup=keyboard
                        )
                except Exception as e:
                    print(f"Error processing link {url}: {e}")
                    continue

        if not any(prefix in start_id for prefix in ['t.me/c/', 't.me/b/']):
            await set_interval(user_id, interval_minutes=20)
            await app.send_message(message.chat.id, "Batch completed successfully by Crushe! 🎉")
            await pin_msg.edit_text(
                        f"Batch process completed for {cl} messages enjoy 🌝\n\n****",
                        reply_markup=keyboard
            )
            return

        data = await db.get_data(user_id)
        if data and data.get("session"):
            session = data.get("session")
            device = 'Vivo Y20'
            userbot = Client(
                ":userbot:",
                api_id=API_ID,
                api_hash=API_HASH,
                device_model=device,
                session_string=session,
                # Add connection settings to improve reliability
                flood_sleep_threshold=SECONDS,  # Sleep on FloodWait up to SECONDS
                retry_delay=1,             # Initial retry delay
                max_retries=5              # Maximum number of retries
            )
            await userbot.start()
        else:
            await app.send_message(message.chat.id, "Login in bot first ...")
            return

        try:
            for i in range(cs, cs + cl):
                if user_id in users_loop and users_loop[user_id]:
                    try:
                        x = start_id.split('/')
                        y = x[:-1]
                        result = '/'.join(y)
                        url = f"{result}/{i}"
                        link = get_link(url)
                        
                        if 't.me/b/' in link or 't.me/c/' in link:
                            msg = await app.send_message(message.chat.id, f"Processing by Crushe...")
                            await safe_execute(process_and_upload_link, userbot, user_id, msg.id, link, 0, message)
                            await pin_msg.edit_text(
                            f"⚡\n__Processing: {i - cs + 1}/{cl}__\n\nBatch process started",
                            reply_markup=keyboard
                            )
                    except Exception as e:
                        print(f"Error processing link {url}: {e}")
                        continue
        finally:
            if userbot.is_connected:
                await userbot.stop()

        await app.send_message(message.chat.id, "Batch completed successfully by Crushe! 🎉")
        await set_interval(user_id, interval_minutes=20)
        await pin_msg.edit_text(
                        f"Batch completed for {cl} messages ⚡\n\n****",
                        reply_markup=keyboard
        )
    except FloodWait as fw:
        wait_time = fw.value if hasattr(fw, 'value') else fw.x
        await app.send_message(
            message.chat.id,
            f"Try again after {wait_time} seconds due to floodwait from Telegram."
        )
        # Sleep and retry automatically
        await asyncio.sleep(wait_time + 1)
        await app.send_message(message.chat.id, "Retrying after FloodWait...")
    except ConnectionError as e:
        await app.send_message(message.chat.id, f"Connection error: {str(e)}. Please try again later.")
    except Timeout as e:
        await app.send_message(message.chat.id, f"Request timed out: {str(e)}. Please try again later.")
    except RequestException as e:
        await app.send_message(message.chat.id, f"Network error: {str(e)}. Please try again later.")
    except Exception as e:
        await app.send_message(message.chat.id, f"Error: {str(e)}")
    finally:
        if 'userbot' in locals() and userbot and userbot.is_connected:
            await userbot.stop()
        users_loop.pop(user_id, None)


@app.on_message(filters.command("cancel"))
async def stop_batch(_, message):
    user_id = message.chat.id
    if user_id in users_loop and users_loop[user_id]:
        users_loop[user_id] = False  
        await app.send_message(
            message.chat.id, 
            "Batch processing has been stopped successfully. You can start a new batch now if you want."
        )
    elif user_id in users_loop and not users_loop[user_id]:
        await app.send_message(
            message.chat.id, 
            "The batch process was already stopped. No active batch to cancel."
        )
    else:
        await app.send_message(
            message.chat.id, 
            "No active batch processing is running to cancel."
        )


# --- Helper and Utility Functions ---

async def check_interval(user_id, freecheck):
    if freecheck != 1 or await is_user_verified(user_id):  
        return True, None

    now = datetime.now()

    if user_id in interval_set:
        cooldown_end = interval_set[user_id]
        if now < cooldown_end:
            remaining_time = (cooldown_end - now).seconds // 60
            return False, f"Please wait {remaining_time} minute(s) before sending another link. Alternatively, purchase premium for instant access.\n\n> Hey 👋 You can use /token to use the bot free for 3 hours without any time limit."
        else:
            del interval_set[user_id]  

    return True, None

async def set_interval(user_id, interval_minutes=5):
    now = datetime.now()
    interval_set[user_id] = now + timedelta(minutes=interval_minutes)

async def process_and_upload_link(userbot, user_id, msg_id, link, retry_count, message):
    try:
        await get_msg(userbot, user_id, msg_id, link, retry_count, message)
        await asyncio.sleep(3.5)
    finally:
        try:
            await app.delete_messages(message.chat.id, msg_id)
        except Exception:
            pass

async def generate_random_name(length=8):
    return ''.join(random.choices(string.ascii_lowercase, k=length))
from crushe import app
from config import API_ID, API_HASH, FREEMIUM_LIMIT, PREMIUM_LIMIT, OWNER_ID
from crushe.core.get_func import get_msg
from crushe.core.func import *
from crushe.core.mongo import db
from crushe.modules.shrink import is_user_verified
from pyrogram.errors import FloodWait
from datetime import datetime, timedelta
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

async def generate_random_name(length=8):
    return ''.join(random.choices(string.ascii_lowercase, k=length))


users_loop = {}
interval_set = {}
batch_mode = {}

async def process_and_upload_link(userbot, user_id, msg_id, link, retry_count, message):
    try:
        # Import error handling utilities
        from crushe.core.error_handler import safe_execute
        # Use safe_execute to handle FloodWait and other errors
        await safe_execute(get_msg, userbot, user_id, msg_id, link, retry_count, message)
        await asyncio.sleep(3.5)
    finally:
        # Auto-delete the "Processing..." message regardless of outcome.
        try:
            await app.delete_messages(message.chat.id, msg_id)
        except Exception:
            pass


async def check_interval(user_id, freecheck):
    if freecheck != 1 or await is_user_verified(user_id):  
        return True, None

    now = datetime.now()

    if user_id in interval_set:
        cooldown_end = interval_set[user_id]
        if now < cooldown_end:
            remaining_time = (cooldown_end - now).seconds // 60
            return False, f"Please wait {remaining_time} minute(s) before sending another link. Alternatively, purchase premium for instant access.\n\n> Hey 👋 You can use /token to use the bot free for 3 hours without any time limit."
        else:
            del interval_set[user_id]  

    return True, None

async def set_interval(user_id, interval_minutes=5):
    now = datetime.now()
    interval_set[user_id] = now + timedelta(minutes=interval_minutes)


@app.on_message(filters.regex(r'https?://(?:www\.)?t\.me/[^\s]+') & filters.private)
@retry_with_backoff(max_retries=5, initial_delay=2.0, max_delay=SECONDS)
async def single_link(_, message):
    from crushe.core.error_handler import retry_with_backoff, safe_execute
    
    user_id = message.chat.id
    if user_id in batch_mode:
        return

    if users_loop.get(user_id, False):
        await message.reply(
            "You already have an ongoing process. Please wait for it to finish or cancel it with /cancel."
        )
        return    

    freecheck = await chk_user(message, user_id)
    if freecheck == 1 and FREEMIUM_LIMIT == 0 and user_id not in OWNER_ID:
        await message.reply("Freemium service is currently not available. Upgrade to premium for access.")
        return

    can_proceed, response_message = await check_interval(user_id, freecheck)
    if not can_proceed:
        await message.reply(response_message)
        return

    users_loop[user_id] = True
    link = get_link(message.text) 
    userbot = None
    msg = None
    try:
        join = await subscribe(_, message)
        if join == 1:
            users_loop[user_id] = False
            return

        msg = await message.reply("Processing...")

        if 't.me/' in link and 't.me/+' not in link and 't.me/c/' not in link and 't.me/b/' not in link:
            data = await db.get_data(user_id)
            if data and data.get("session"):
                session = data.get("session")
                try:
                    device = 'Vivo Y20'
                    userbot = Client(
                        ":userbot:",
                        api_id=API_ID,
                        api_hash=API_HASH,
                        device_model=device,
                        session_string=session
                    )
                    await userbot.start()
                except Exception as e:
                    userbot = None
            else:
                userbot = None

            # Use safe_execute to handle FloodWait and other errors
            await safe_execute(process_and_upload_link, userbot, user_id, msg.id, link, 0, message)
            await set_interval(user_id, interval_minutes=5)
            users_loop[user_id] = False
            return

        data = await db.get_data(user_id)

        if data and data.get("session"):
            session = data.get("session")
            try:
                device = 'Vivo Y20'
                userbot = Client(
                    ":userbot:", 
                    api_id=API_ID, 
                    api_hash=API_HASH, 
                    device_model=device, 
                    session_string=session,
                    # Add connection settings to improve reliability
                    flood_sleep_threshold=60,  # Sleep on FloodWait up to 60 seconds
                    retry_delay=1,             # Initial retry delay
                    max_retries=5              # Maximum number of retries
                )
                await userbot.start()                
            except Exception as e:
                users_loop[user_id] = False
                return await msg.edit_text(f"Login expired /login again... Error: {str(e)}")
        else:
            users_loop[user_id] = False
            await msg.edit_text("Login in bot first ...")
            return

        try:
            if 't.me/+' in link:
                # Use retry_with_backoff for userbot_join
                @retry_with_backoff(max_retries=3)
                async def join_with_retry():
                    return await userbot_join(userbot, link)
                
                q = await join_with_retry()
                await msg.edit_text(q)
            elif 't.me/c/' in link:
                # Use safe_execute to handle FloodWait and other errors
                await safe_execute(process_and_upload_link, userbot, user_id, msg.id, link, 0, message)
                await set_interval(user_id, interval_minutes=5)
            else:
                await msg.edit_text("Invalid link format.")
        except Exception as e:
            await msg.edit_text(f"Link: `{link}`\n\n**Error:** {str(e)}")

    except FloodWait as fw:
        wait_time = fw.value if hasattr(fw, 'value') else fw.x
        await msg.edit_text(f'Try again after {wait_time} seconds due to floodwait from telegram.')
        # Sleep for the required time and then retry automatically
        await asyncio.sleep(wait_time + 1)
        if msg:
            await msg.edit_text("Retrying after FloodWait...")
            # Retry the operation after waiting
            try:
                if 't.me/c/' in link:
                    await safe_execute(process_and_upload_link, userbot, user_id, msg.id, link, 0, message)
            except Exception as e:
                await msg.edit_text(f"Retry failed: {str(e)}")

    except ConnectionError as e:
        if msg:
            await msg.edit_text(f"Connection error: {str(e)}. Please try again later.")
    except Timeout as e:
        if msg:
            await msg.edit_text(f"Request timed out: {str(e)}. Please try again later.")
    except RequestException as e:
        if msg:
            await msg.edit_text(f"Network error: {str(e)}. Please try again later.")
    except UserNotParticipant:
        if msg:
            await msg.edit_text("You need to join the required channel first.")
    except Exception as e:
        if msg:
            await msg.edit_text(f"Link: `{link}`\n\n**Error:** {str(e)}")
    finally:
        if userbot and userbot.is_connected:  
            await userbot.stop()
        users_loop[user_id] = False  




@app.on_message(filters.command("batch") & filters.private)
@retry_with_backoff(max_retries=5, initial_delay=2.0, max_delay=SECONDS)
async def batch_link(_, message):
    user_id = message.chat.id

    if users_loop.get(user_id, False):  
        await app.send_message(
            message.chat.id,
            "You already have a batch process running. Please wait for it to complete before starting a new one."
        )
        return

    freecheck = await chk_user(message, user_id)
    if freecheck == 1 and FREEMIUM_LIMIT == 0 and user_id not in OWNER_ID:
        await message.reply("Freemium service is currently not available. Upgrade to premium for access.")
        return

    toker = await is_user_verified(user_id)
    if toker:
        max_batch_size = (FREEMIUM_LIMIT + 1)
        freecheck = 0  
    else:
        freecheck = await chk_user(message, user_id)
        if freecheck == 1:
            max_batch_size = FREEMIUM_LIMIT  
        else:
            max_batch_size = PREMIUM_LIMIT

    attempts = 0
    while attempts < 3:
        start = await app.ask(message.chat.id, text="Please send the start link.")
        start_id = start.text.strip()
        s = start_id.split("/")[-1]  
        try:
            cs = int(s)  
            break  
        except ValueError:
            attempts += 1
            if attempts == 3:
                await app.send_message(message.chat.id, "You have exceeded the maximum number of attempts. Please try again later.")
                return
            await app.send_message(message.chat.id, "Invalid link. Please send again ...")

    attempts = 0
    while attempts < 3:
        num_messages = await app.ask(message.chat.id, text="How many messages do you want to process?")
        try:
            cl = int(num_messages.text.strip())  
            if cl <= 0 or cl > max_batch_size:
                raise ValueError(f"Number of messages must be between 1 and {max_batch_size}.")
            break  
        except ValueError as e:
            attempts += 1
            if attempts == 3:
                await app.send_message(message.chat.id, "You have exceeded the maximum number of attempts. Please try again later.")
                return
            await app.send_message(message.chat.id, f"Invalid number: {e}. Please enter a valid number again ...")

    can_proceed, response_message = await check_interval(user_id, freecheck)
    if not can_proceed:
        await message.reply(response_message)
        return

    join_button = InlineKeyboardButton("Join Channel", url="https://t.me/+3bMBj190KOc3YzNk")
    keyboard = InlineKeyboardMarkup([[join_button]])

    pin_msg = await app.send_message(
        user_id,
        "⚡\n__Processing: 0/{cl}__\n\nBatch process started",
        reply_markup=keyboard
    )
    try:
        await pin_msg.pin()
    except Exception as e:
        await pin_msg.pin(both_sides=True)
    
    users_loop[user_id] = True
    try:
        for i in range(cs, cs + cl):
            if user_id in users_loop and users_loop[user_id]:
                try:
                    x = start_id.split('/')
                    y = x[:-1]
                    result = '/'.join(y)
                    url = f"{result}/{i}"
                    link = get_link(url)
                    
                    if 't.me/' in link and 't.me/b/' not in link and 't.me/c' not in link:
                        userbot = None
                        data = await db.get_data(user_id)
                        if data and data.get("session"):
                            session = data.get("session")
                            try:
                                device = 'Vivo Y20'
                                userbot = Client(
                                    ":userbot:",
                                    api_id=API_ID,
                                    api_hash=API_HASH,
                                    device_model=device,
                                    session_string=session,
                                    # Add connection settings to improve reliability
                                    flood_sleep_threshold=SECONDS,  # Sleep on FloodWait up to SECONDS
                                    retry_delay=1,             # Initial retry delay
                                    max_retries=5              # Maximum number of retries
                                )
                                await userbot.start()
                            except Exception as e:
                                userbot = None
                        else:
                            userbot = None
                        msg = await app.send_message(message.chat.id, f"Processing Crushe...")
                        await safe_execute(process_and_upload_link, userbot, user_id, msg.id, link, 0, message)
                        await pin_msg.edit_text(
                        f"⚡\n__Processing: {i - cs + 1}/{cl}__\n\nBatch process started",
                        reply_markup=keyboard
                        )
                except Exception as e:
                    print(f"Error processing link {url}: {e}")
                    continue

        if not any(prefix in start_id for prefix in ['t.me/c/', 't.me/b/']):
            await set_interval(user_id, interval_minutes=20)
            await app.send_message(message.chat.id, "Batch completed successfully by Crushe! 🎉")
            await pin_msg.edit_text(
                        f"Batch process completed for {cl} messages enjoy 🌝\n\n****",
                        reply_markup=keyboard
            )
            return

        data = await db.get_data(user_id)
        if data and data.get("session"):
            session = data.get("session")
            device = 'Vivo Y20'
            userbot = Client(
                ":userbot:",
                api_id=API_ID,
                api_hash=API_HASH,
                device_model=device,
                session_string=session,
                # Add connection settings to improve reliability
                flood_sleep_threshold=SECONDS,  # Sleep on FloodWait up to SECONDS
                retry_delay=1,             # Initial retry delay
                max_retries=5              # Maximum number of retries
            )
            await userbot.start()
        else:
            await app.send_message(message.chat.id, "Login in bot first ...")
            return

        try:
            for i in range(cs, cs + cl):
                if user_id in users_loop and users_loop[user_id]:
                    try:
                        x = start_id.split('/')
                        y = x[:-1]
                        result = '/'.join(y)
                        url = f"{result}/{i}"
                        link = get_link(url)
                        
                        if 't.me/b/' in link or 't.me/c/' in link:
                            msg = await app.send_message(message.chat.id, f"Processing by Crushe...")
                            await safe_execute(process_and_upload_link, userbot, user_id, msg.id, link, 0, message)
                            await pin_msg.edit_text(
                            f"⚡\n__Processing: {i - cs + 1}/{cl}__\n\nBatch process started",
                            reply_markup=keyboard
                            )
                    except Exception as e:
                        print(f"Error processing link {url}: {e}")
                        continue
        finally:
            if userbot.is_connected:
                await userbot.stop()

        await app.send_message(message.chat.id, "Batch completed successfully by Crushe! 🎉")
        await set_interval(user_id, interval_minutes=20)
        await pin_msg.edit_text(
                        f"Batch completed for {cl} messages ⚡\n\n****",
                        reply_markup=keyboard
        )
    except FloodWait as fw:
        wait_time = fw.value if hasattr(fw, 'value') else fw.x
        await app.send_message(
            message.chat.id,
            f"Try again after {wait_time} seconds due to floodwait from Telegram."
        )
        # Sleep and retry automatically
        await asyncio.sleep(wait_time + 1)
        await app.send_message(message.chat.id, "Retrying after FloodWait...")
    except ConnectionError as e:
        await app.send_message(message.chat.id, f"Connection error: {str(e)}. Please try again later.")
    except Timeout as e:
        await app.send_message(message.chat.id, f"Request timed out: {str(e)}. Please try again later.")
    except RequestException as e:
        await app.send_message(message.chat.id, f"Network error: {str(e)}. Please try again later.")
    except Exception as e:
        await app.send_message(message.chat.id, f"Error: {str(e)}")
    finally:
        if 'userbot' in locals() and userbot and userbot.is_connected:
            await userbot.stop()
        users_loop.pop(user_id, None)


@app.on_message(filters.command("cancel"))
async def stop_batch(_, message):
    user_id = message.chat.id
    if user_id in users_loop and users_loop[user_id]:
        users_loop[user_id] = False  
        await app.send_message(
            message.chat.id, 
            "Batch processing has been stopped successfully. You can start a new batch now if you want."
        )
    elif user_id in users_loop and not users_loop[user_id]:
        await app.send_message(
            message.chat.id, 
            "The batch process was already stopped. No active batch to cancel."
        )
    else:
        await app.send_message(
            message.chat.id, 
            "No active batch processing is running to cancel."
        )


# --- Helper and Utility Functions ---

async def check_interval(user_id, freecheck):
    if freecheck != 1 or await is_user_verified(user_id):  
        return True, None

    now = datetime.now()

    if user_id in interval_set:
        cooldown_end = interval_set[user_id]
        if now < cooldown_end:
            remaining_time = (cooldown_end - now).seconds // 60
            return False, f"Please wait {remaining_time} minute(s) before sending another link. Alternatively, purchase premium for instant access.\n\n> Hey 👋 You can use /token to use the bot free for 3 hours without any time limit."
        else:
            del interval_set[user_id]  

    return True, None

async def set_interval(user_id, interval_minutes=5):
    now = datetime.now()
    interval_set[user_id] = now + timedelta(minutes=interval_minutes)

async def process_and_upload_link(userbot, user_id, msg_id, link, retry_count, message):
    try:
        await get_msg(userbot, user_id, msg_id, link, retry_count, message)
        await asyncio.sleep(3.5)
    finally:
        try:
            await app.delete_messages(message.chat.id, msg_id)
        except Exception:
            pass

async def generate_random_name(length=8):
    return ''.join(random.choices(string.ascii_lowercase, k=length))
from crushe import app
from config import API_ID, API_HASH, FREEMIUM_LIMIT, PREMIUM_LIMIT, OWNER_ID
from crushe.core.get_func import get_msg
from crushe.core.func import *
from crushe.core.mongo import db
from crushe.modules.shrink import is_user_verified
from pyrogram.errors import FloodWait
from datetime import datetime, timedelta
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

async def generate_random_name(length=8):
    return ''.join(random.choices(string.ascii_lowercase, k=length))


users_loop = {}
interval_set = {}
batch_mode = {}

async def process_and_upload_link(userbot, user_id, msg_id, link, retry_count, message):
    try:
        # Import error handling utilities
        from crushe.core.error_handler import safe_execute
        # Use safe_execute to handle FloodWait and other errors
        await safe_execute(get_msg, userbot, user_id, msg_id, link, retry_count, message)
        await asyncio.sleep(3.5)
    finally:
        # Auto-delete the "Processing..." message regardless of outcome.
        try:
            await app.delete_messages(message.chat.id, msg_id)
        except Exception:
            pass


async def check_interval(user_id, freecheck):
    if freecheck != 1 or await is_user_verified(user_id):  
        return True, None

    now = datetime.now()

    if user_id in interval_set:
        cooldown_end = interval_set[user_id]
        if now < cooldown_end:
            remaining_time = (cooldown_end - now).seconds // 60
            return False, f"Please wait {remaining_time} minute(s) before sending another link. Alternatively, purchase premium for instant access.\n\n> Hey 👋 You can use /token to use the bot free for 3 hours without any time limit."
        else:
            del interval_set[user_id]  

    return True, None

async def set_interval(user_id, interval_minutes=5):
    now = datetime.now()
    interval_set[user_id] = now + timedelta(minutes=interval_minutes)


@app.on_message(filters.regex(r'https?://(?:www\.)?t\.me/[^\s]+') & filters.private)
@retry_with_backoff(max_retries=5, initial_delay=2.0, max_delay=SECONDS)
async def single_link(_, message):
    from crushe.core.error_handler import retry_with_backoff, safe_execute
    
    user_id = message.chat.id
    if user_id in batch_mode:
        return

    if users_loop.get(user_id, False):
        await message.reply(
            "You already have an ongoing process. Please wait for it to finish or cancel it with /cancel."
        )
        return    

    freecheck = await chk_user(message, user_id)
    if freecheck == 1 and FREEMIUM_LIMIT == 0 and user_id not in OWNER_ID:
        await message.reply("Freemium service is currently not available. Upgrade to premium for access.")
        return

    can_proceed, response_message = await check_interval(user_id, freecheck)
    if not can_proceed:
        await message.reply(response_message)
        return

    users_loop[user_id] = True
    link = get_link(message.text) 
    userbot = None
    msg = None
    try:
        join = await subscribe(_, message)
        if join == 1:
            users_loop[user_id] = False
            return

        msg = await message.reply("Processing...")

        if 't.me/' in link and 't.me/+' not in link and 't.me/c/' not in link and 't.me/b/' not in link:
            data = await db.get_data(user_id)
            if data and data.get("session"):
                session = data.get("session")
                try:
                    device = 'Vivo Y20'
                    userbot = Client(
                        ":userbot:",
                        api_id=API_ID,
                        api_hash=API_HASH,
                        device_model=device,
                        session_string=session,
                        # Add connection settings to improve reliability
                        flood_sleep_threshold=SECONDS,  # Sleep on FloodWait up to SECONDS
                        retry_delay=2,             # Initial retry delay
                        max_retries=5,             # Maximum number of retries
                        no_updates=True,           # Disable updates to reduce overhead
                        workers=4                  # Limit number of workers to prevent overloading
                    )
                    await userbot.start()
                except Exception as e:
                    userbot = None
            else:
                userbot = None

            # Use safe_execute to handle FloodWait and other errors
            await safe_execute(process_and_upload_link, userbot, user_id, msg.id, link, 0, message)
            await set_interval(user_id, interval_minutes=5)
            users_loop[user_id] = False
            return

        data = await db.get_data(user_id)

        if data and data.get("session"):
            session = data.get("session")
            try:
                device = 'Vivo Y20'
                userbot = Client(
                    ":userbot:", 
                    api_id=API_ID, 
                    api_hash=API_HASH, 
                    device_model=device, 
                    session_string=session,
                    # Add connection settings to improve reliability
                    flood_sleep_threshold=60,  # Sleep on FloodWait up to 60 seconds
                    retry_delay=1,             # Initial retry delay
                    max_retries=5              # Maximum number of retries
                )
                await userbot.start()                
            except Exception as e:
                users_loop[user_id] = False
                return await msg.edit_text(f"Login expired /login again... Error: {str(e)}")
        else:
            users_loop[user_id] = False
            await msg.edit_text("Login in bot first ...")
            return

        try:
            if 't.me/+' in link:
                # Use retry_with_backoff for userbot_join
                @retry_with_backoff(max_retries=3)
                async def join_with_retry():
                    return await userbot_join(userbot, link)
                
                q = await join_with_retry()
                await msg.edit_text(q)
            elif 't.me/c/' in link:
                # Use safe_execute to handle FloodWait and other errors
                await safe_execute(process_and_upload_link, userbot, user_id, msg.id, link, 0, message)
                await set_interval(user_id, interval_minutes=5)
            else:
                await msg.edit_text("Invalid link format.")
        except Exception as e:
            await msg.edit_text(f"Link: `{link}`\n\n**Error:** {str(e)}")

    except FloodWait as fw:
        wait_time = fw.value if hasattr(fw, 'value') else fw.x
        await msg.edit_text(f'Try again after {wait_time} seconds due to floodwait from telegram.')
        # Sleep for the required time and then retry automatically
        await asyncio.sleep(wait_time + 1)
        if msg:
            await msg.edit_text("Retrying after FloodWait...")
            # Retry the operation after waiting
            try:
                if 't.me/c/' in link:
                    await safe_execute(process_and_upload_link, userbot, user_id, msg.id, link, 0, message)
            except Exception as e:
                await msg.edit_text(f"Retry failed: {str(e)}")

    except ConnectionError as e:
        if msg:
            await msg.edit_text(f"Connection error: {str(e)}. Please try again later.")
    except Timeout as e:
        if msg:
            await msg.edit_text(f"Request timed out: {str(e)}. Please try again later.")
    except RequestException as e:
        if msg:
            await msg.edit_text(f"Network error: {str(e)}. Please try again later.")
    except UserNotParticipant:
        if msg:
            await msg.edit_text("You need to join the required channel first.")
    except Exception as e:
        if msg:
            await msg.edit_text(f"Link: `{link}`\n\n**Error:** {str(e)}")
    finally:
        if userbot and userbot.is_connected:  
            await userbot.stop()
        users_loop[user_id] = False  




@app.on_message(filters.command("batch") & filters.private)
@retry_with_backoff(max_retries=5, initial_delay=2.0, max_delay=SECONDS)
async def batch_link(_, message):
    user_id = message.chat.id

    if users_loop.get(user_id, False):  
        await app.send_message(
            message.chat.id,
            "You already have a batch process running. Please wait for it to complete before starting a new one."
        )
        return

    freecheck = await chk_user(message, user_id)
    if freecheck == 1 and FREEMIUM_LIMIT == 0 and user_id not in OWNER_ID:
        await message.reply("Freemium service is currently not available. Upgrade to premium for access.")
        return

    toker = await is_user_verified(user_id)
    if toker:
        max_batch_size = (FREEMIUM_LIMIT + 1)
        freecheck = 0  
    else:
        freecheck = await chk_user(message, user_id)
        if freecheck == 1:
            max_batch_size = FREEMIUM_LIMIT  
        else:
            max_batch_size = PREMIUM_LIMIT

    attempts = 0
    while attempts < 3:
        start = await app.ask(message.chat.id, text="Please send the start link.")
        start_id = start.text.strip()
        s = start_id.split("/")[-1]  
        try:
            cs = int(s)  
            break  
        except ValueError:
            attempts += 1
            if attempts == 3:
                await app.send_message(message.chat.id, "You have exceeded the maximum number of attempts. Please try again later.")
                return
            await app.send_message(message.chat.id, "Invalid link. Please send again ...")

    attempts = 0
    while attempts < 3:
        num_messages = await app.ask(message.chat.id, text="How many messages do you want to process?")
        try:
            cl = int(num_messages.text.strip())  
            if cl <= 0 or cl > max_batch_size:
                raise ValueError(f"Number of messages must be between 1 and {max_batch_size}.")
            break  
        except ValueError as e:
            attempts += 1
            if attempts == 3:
                await app.send_message(message.chat.id, "You have exceeded the maximum number of attempts. Please try again later.")
                return
            await app.send_message(message.chat.id, f"Invalid number: {e}. Please enter a valid number again ...")

    can_proceed, response_message = await check_interval(user_id, freecheck)
    if not can_proceed:
        await message.reply(response_message)
        return

    join_button = InlineKeyboardButton("Join Channel", url="https://t.me/+3bMBj190KOc3YzNk")
    keyboard = InlineKeyboardMarkup([[join_button]])

    pin_msg = await app.send_message(
        user_id,
        "⚡\n__Processing: 0/{cl}__\n\nBatch process started",
        reply_markup=keyboard
    )
    try:
        await pin_msg.pin()
    except Exception as e:
        await pin_msg.pin(both_sides=True)
    
    users_loop[user_id] = True
    try:
        for i in range(cs, cs + cl):
            if user_id in users_loop and users_loop[user_id]:
                try:
                    x = start_id.split('/')
                    y = x[:-1]
                    result = '/'.join(y)
                    url = f"{result}/{i}"
                    link = get_link(url)
                    
                    if 't.me/' in link and 't.me/b/' not in link and 't.me/c' not in link:
                        userbot = None
                        data = await db.get_data(user_id)
                        if data and data.get("session"):
                            session = data.get("session")
                            try:
                                device = 'Vivo Y20'
                                userbot = Client(
                                    ":userbot:",
                                    api_id=API_ID,
                                    api_hash=API_HASH,
                                    device_model=device,
                                    session_string=session,
                                    # Add connection settings to improve reliability
                                    flood_sleep_threshold=SECONDS,  # Sleep on FloodWait up to SECONDS
                                    retry_delay=1,             # Initial retry delay
                                    max_retries=5              # Maximum number of retries
                                )
                                await userbot.start()
                            except Exception as e:
                                userbot = None
                        else:
                            userbot = None
                        msg = await app.send_message(message.chat.id, f"Processing Crushe...")
                        await safe_execute(process_and_upload_link, userbot, user_id, msg.id, link, 0, message)
                        await pin_msg.edit_text(
                        f"⚡\n__Processing: {i - cs + 1}/{cl}__\n\nBatch process started",
                        reply_markup=keyboard
                        )
                except Exception as e:
                    print(f"Error processing link {url}: {e}")
                    continue

        if not any(prefix in start_id for prefix in ['t.me/c/', 't.me/b/']):
            await set_interval(user_id, interval_minutes=20)
            await app.send_message(message.chat.id, "Batch completed successfully by Crushe! 🎉")
            await pin_msg.edit_text(
                        f"Batch process completed for {cl} messages enjoy 🌝\n\n****",
                        reply_markup=keyboard
            )
            return

        data = await db.get_data(user_id)
        if data and data.get("session"):
            session = data.get("session")
            device = 'Vivo Y20'
            userbot = Client(
                ":userbot:",
                api_id=API_ID,
                api_hash=API_HASH,
                device_model=device,
                session_string=session,
                # Add connection settings to improve reliability
                flood_sleep_threshold=SECONDS,  # Sleep on FloodWait up to SECONDS
                retry_delay=1,             # Initial retry delay
                max_retries=5              # Maximum number of retries
            )
            await userbot.start()
        else:
            await app.send_message(message.chat.id, "Login in bot first ...")
            return

        try:
            for i in range(cs, cs + cl):
                if user_id in users_loop and users_loop[user_id]:
                    try:
                        x = start_id.split('/')
                        y = x[:-1]
                        result = '/'.join(y)
                        url = f"{result}/{i}"
                        link = get_link(url)
                        
                        if 't.me/b/' in link or 't.me/c/' in link:
                            msg = await app.send_message(message.chat.id, f"Processing by Crushe...")
                            await safe_execute(process_and_upload_link, userbot, user_id, msg.id, link, 0, message)
                            await pin_msg.edit_text(
                            f"⚡\n__Processing: {i - cs + 1}/{cl}__\n\nBatch process started",
                            reply_markup=keyboard
                            )
                    except Exception as e:
                        print(f"Error processing link {url}: {e}")
                        continue
        finally:
            if userbot.is_connected:
                await userbot.stop()

        await app.send_message(message.chat.id, "Batch completed successfully by Crushe! 🎉")
        await set_interval(user_id, interval_minutes=20)
        await pin_msg.edit_text(
                        f"Batch completed for {cl} messages ⚡\n\n****",
                        reply_markup=keyboard
        )
    except FloodWait as fw:
        wait_time = fw.value if hasattr(fw, 'value') else fw.x
        await app.send_message(
            message.chat.id,
            f"Try again after {wait_time} seconds due to floodwait from Telegram."
        )
        # Sleep and retry automatically
        await asyncio.sleep(wait_time + 1)
        await app.send_message(message.chat.id, "Retrying after FloodWait...")
    except ConnectionError as e:
        await app.send_message(message.chat.id, f"Connection error: {str(e)}. Please try again later.")
    except Timeout as e:
        await app.send_message(message.chat.id, f"Request timed out: {str(e)}. Please try again later.")
    except RequestException as e:
        await app.send_message(message.chat.id, f"Network error: {str(e)}. Please try again later.")
    except Exception as e:
        await app.send_message(message.chat.id, f"Error: {str(e)}")
    finally:
        if 'userbot' in locals() and userbot and userbot.is_connected:
            await userbot.stop()
        users_loop.pop(user_id, None)


@app.on_message(filters.command("cancel"))
async def stop_batch(_, message):
    user_id = message.chat.id
    if user_id in users_loop and users_loop[user_id]:
        users_loop[user_id] = False  
        await app.send_message(
            message.chat.id, 
            "Batch processing has been stopped successfully. You can start a new batch now if you want."
        )
    elif user_id in users_loop and not users_loop[user_id]:
        await app.send_message(
            message.chat.id, 
            "The batch process was already stopped. No active batch to cancel."
        )
    else:
        await app.send_message(
            message.chat.id, 
            "No active batch processing is running to cancel."
        )


# --- Helper and Utility Functions ---

async def check_interval(user_id, freecheck):
    if freecheck != 1 or await is_user_verified(user_id):  
        return True, None

    now = datetime.now()

    if user_id in interval_set:
        cooldown_end = interval_set[user_id]
        if now < cooldown_end:
            remaining_time = (cooldown_end - now).seconds // 60
            return False, f"Please wait {remaining_time} minute(s) before sending another link. Alternatively, purchase premium for instant access.\n\n> Hey 👋 You can use /token to use the bot free for 3 hours without any time limit."
        else:
            del interval_set[user_id]  

    return True, None

async def set_interval(user_id, interval_minutes=5):
    now = datetime.now()
    interval_set[user_id] = now + timedelta(minutes=interval_minutes)

async def process_and_upload_link(userbot, user_id, msg_id, link, retry_count, message):
    try:
        await get_msg(userbot, user_id, msg_id, link, retry_count, message)
        await asyncio.sleep(3.5)
    finally:
        try:
            await app.delete_messages(message.chat.id, msg_id)
        except Exception:
            pass

async def generate_random_name(length=8):
    return ''.join(random.choices(string.ascii_lowercase, k=length))
from crushe import app
from config import API_ID, API_HASH, FREEMIUM_LIMIT, PREMIUM_LIMIT, OWNER_ID
from crushe.core.get_func import get_msg
from crushe.core.func import *
from crushe.core.mongo import db
from crushe.modules.shrink import is_user_verified
from pyrogram.errors import FloodWait
from datetime import datetime, timedelta
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

async def generate_random_name(length=8):
    return ''.join(random.choices(string.ascii_lowercase, k=length))


users_loop = {}
interval_set = {}
batch_mode = {}

async def process_and_upload_link(userbot, user_id, msg_id, link, retry_count, message):
    try:
        # Import error handling utilities
        from crushe.core.error_handler import safe_execute
        # Use safe_execute to handle FloodWait and other errors
        await safe_execute(get_msg, userbot, user_id, msg_id, link, retry_count, message)
        await asyncio.sleep(3.5)
    finally:
        # Auto-delete the "Processing..." message regardless of outcome.
        try:
            await app.delete_messages(message.chat.id, msg_id)
        except Exception:
            pass


async def check_interval(user_id, freecheck):
    if freecheck != 1 or await is_user_verified(user_id):  
        return True, None

    now = datetime.now()

    if user_id in interval_set:
        cooldown_end = interval_set[user_id]
        if now < cooldown_end:
            remaining_time = (cooldown_end - now).seconds // 60
            return False, f"Please wait {remaining_time} minute(s) before sending another link. Alternatively, purchase premium for instant access.\n\n> Hey 👋 You can use /token to use the bot free for 3 hours without any time limit."
        else:
            del interval_set[user_id]  

    return True, None

async def set_interval(user_id, interval_minutes=5):
    now = datetime.now()
    interval_set[user_id] = now + timedelta(minutes=interval_minutes)


@app.on_message(filters.regex(r'https?://(?:www\.)?t\.me/[^\s]+') & filters.private)
@retry_with_backoff(max_retries=5, initial_delay=2.0, max_delay=SECONDS)
async def single_link(_, message):
    from crushe.core.error_handler import retry_with_backoff, safe_execute
    
    user_id = message.chat.id
    if user_id in batch_mode:
        return

    if users_loop.get(user_id, False):
        await message.reply(
            "You already have an ongoing process. Please wait for it to finish or cancel it with /cancel."
        )
        return    

    freecheck = await chk_user(message, user_id)
    if freecheck == 1 and FREEMIUM_LIMIT == 0 and user_id not in OWNER_ID:
        await message.reply("Freemium service is currently not available. Upgrade to premium for access.")
        return

    can_proceed, response_message = await check_interval(user_id, freecheck)
    if not can_proceed:
        await message.reply(response_message)
        return

    users_loop[user_id] = True
    link = get_link(message.text) 
    userbot = None
    msg = None
    try:
        join = await subscribe(_, message)
        if join == 1:
            users_loop[user_id] = False
            return

        msg = await message.reply("Processing...")

        if 't.me/' in link and 't.me/+' not in link and 't.me/c/' not in link and 't.me/b/' not in link:
            data = await db.get_data(user_id)
            if data and data.get("session"):
                session = data.get("session")
                try:
                    device = 'Vivo Y20'
                    userbot = Client(
                        ":userbot:",
                        api_id=API_ID,
                        api_hash=API_HASH,
                        device_model=device,
                        session_string=session
                    )
                    await userbot.start()
                except Exception as e:
                    userbot = None
            else:
                userbot = None

            # Use safe_execute to handle FloodWait and other errors
            await safe_execute(process_and_upload_link, userbot, user_id, msg.id, link, 0, message)
            await set_interval(user_id, interval_minutes=5)
            users_loop[user_id] = False
            return

        data = await db.get_data(user_id)

        if data and data.get("session"):
            session = data.get("session")
            try:
                device = 'Vivo Y20'
                userbot = Client(
                    ":userbot:", 
                    api_id=API_ID, 
                    api_hash=API_HASH, 
                    device_model=device, 
                    session_string=session,
                    # Add connection settings to improve reliability
                    flood_sleep_threshold=60,  # Sleep on FloodWait up to 60 seconds
                    retry_delay=1,             # Initial retry delay
                    max_retries=5              # Maximum number of retries
                )
                await userbot.start()                
            except Exception as e:
                users_loop[user_id] = False
                return await msg.edit_text(f"Login expired /login again... Error: {str(e)}")
        else:
            users_loop[user_id] = False
            await msg.edit_text("Login in bot first ...")
            return

        try:
            if 't.me/+' in link:
                # Use retry_with_backoff for userbot_join
                @retry_with_backoff(max_retries=3)
                async def join_with_retry():
                    return await userbot_join(userbot, link)
                
                q = await join_with_retry()
                await msg.edit_text(q)
            elif 't.me/c/' in link:
                # Use safe_execute to handle FloodWait and other errors
                await safe_execute(process_and_upload_link, userbot, user_id, msg.id, link, 0, message)
                await set_interval(user_id, interval_minutes=5)
            else:
                await msg.edit_text("Invalid link format.")
        except Exception as e:
            await msg.edit_text(f"Link: `{link}`\n\n**Error:** {str(e)}")

    except FloodWait as fw:
        wait_time = fw.value if hasattr(fw, 'value') else fw.x
        await msg.edit_text(f'Try again after {wait_time} seconds due to floodwait from telegram.')
        # Sleep for the required time and then retry automatically
        await asyncio.sleep(wait_time + 1)
        if msg:
            await msg.edit_text("Retrying after FloodWait...")
            # Retry the operation after waiting
            try:
                if 't.me/c/' in link:
                    await safe_execute(process_and_upload_link, userbot, user_id, msg.id, link, 0, message)
            except Exception as e:
                await msg.edit_text(f"Retry failed: {str(e)}")

    except ConnectionError as e:
        if msg:
            await msg.edit_text(f"Connection error: {str(e)}. Please try again later.")
    except Timeout as e:
        if msg:
            await msg.edit_text(f"Request timed out: {str(e)}. Please try again later.")
    except RequestException as e:
        if msg:
            await msg.edit_text(f"Network error: {str(e)}. Please try again later.")
    except UserNotParticipant:
        if msg:
            await msg.edit_text("You need to join the required channel first.")
    except Exception as e:
        if msg:
            await msg.edit_text(f"Link: `{link}`\n\n**Error:** {str(e)}")
    finally:
        if userbot and userbot.is_connected:  
            await userbot.stop()
        users_loop[user_id] = False  




@app.on_message(filters.command("batch") & filters.private)
@retry_with_backoff(max_retries=5, initial_delay=2.0, max_delay=SECONDS)
async def batch_link(_, message):
    user_id = message.chat.id

    if users_loop.get(user_id, False):  
        await app.send_message(
            message.chat.id,
            "You already have a batch process running. Please wait for it to complete before starting a new one."
        )
        return

    freecheck = await chk_user(message, user_id)
    if freecheck == 1 and FREEMIUM_LIMIT == 0 and user_id not in OWNER_ID:
        await message.reply("Freemium service is currently not available. Upgrade to premium for access.")
        return

    toker = await is_user_verified(user_id)
    if toker:
        max_batch_size = (FREEMIUM_LIMIT + 1)
        freecheck = 0  
    else:
        freecheck = await chk_user(message, user_id)
        if freecheck == 1:
            max_batch_size = FREEMIUM_LIMIT  
        else:
            max_batch_size = PREMIUM_LIMIT

    attempts = 0
    while attempts < 3:
        start = await app.ask(message.chat.id, text="Please send the start link.")
        start_id = start.text.strip()
        s = start_id.split("/")[-1]  
        try:
            cs = int(s)  
            break  
        except ValueError:
            attempts += 1
            if attempts == 3:
                await app.send_message(message.chat.id, "You have exceeded the maximum number of attempts. Please try again later.")
                return
            await app.send_message(message.chat.id, "Invalid link. Please send again ...")

    attempts = 0
    while attempts < 3:
        num_messages = await app.ask(message.chat.id, text="How many messages do you want to process?")
        try:
            cl = int(num_messages.text.strip())  
            if cl <= 0 or cl > max_batch_size:
                raise ValueError(f"Number of messages must be between 1 and {max_batch_size}.")
            break  
        except ValueError as e:
            attempts += 1
            if attempts == 3:
                await app.send_message(message.chat.id, "You have exceeded the maximum number of attempts. Please try again later.")
                return
            await app.send_message(message.chat.id, f"Invalid number: {e}. Please enter a valid number again ...")

    can_proceed, response_message = await check_interval(user_id, freecheck)
    if not can_proceed:
        await message.reply(response_message)
        return

    join_button = InlineKeyboardButton("Join Channel", url="https://t.me/+3bMBj190KOc3YzNk")
    keyboard = InlineKeyboardMarkup([[join_button]])

    pin_msg = await app.send_message(
        user_id,
        "⚡\n__Processing: 0/{cl}__\n\nBatch process started",
        reply_markup=keyboard
    )
    try:
        await pin_msg.pin()
    except Exception as e:
        await pin_msg.pin(both_sides=True)
    
    users_loop[user_id] = True
    try:
        for i in range(cs, cs + cl):
            if user_id in users_loop and users_loop[user_id]:
                try:
                    x = start_id.split('/')
                    y = x[:-1]
                    result = '/'.join(y)
                    url = f"{result}/{i}"
                    link = get_link(url)
                    
                    if 't.me/' in link and 't.me/b/' not in link and 't.me/c' not in link:
                        userbot = None
                        data = await db.get_data(user_id)
                        if data and data.get("session"):
                            session = data.get("session")
                            try:
                                device = 'Vivo Y20'
                                userbot = Client(
                                    ":userbot:",
                                    api_id=API_ID,
                                    api_hash=API_HASH,
                                    device_model=device,
                                    session_string=session,
                                    # Add connection settings to improve reliability
                                    flood_sleep_threshold=SECONDS,  # Sleep on FloodWait up to SECONDS
                                    retry_delay=1,             # Initial retry delay
                                    max_retries=5              # Maximum number of retries
                                )
                                await userbot.start()
                            except Exception as e:
                                userbot = None
                        else:
                            userbot = None
                        msg = await app.send_message(message.chat.id, f"Processing Crushe...")
                        await safe_execute(process_and_upload_link, userbot, user_id, msg.id, link, 0, message)
                        await pin_msg.edit_text(
                        f"⚡\n__Processing: {i - cs + 1}/{cl}__\n\nBatch process started",
                        reply_markup=keyboard
                        )
                except Exception as e:
                    print(f"Error processing link {url}: {e}")
                    continue

        if not any(prefix in start_id for prefix in ['t.me/c/', 't.me/b/']):
            await set_interval(user_id, interval_minutes=20)
            await app.send_message(message.chat.id, "Batch completed successfully by Crushe! 🎉")
            await pin_msg.edit_text(
                        f"Batch process completed for {cl} messages enjoy 🌝\n\n****",
                        reply_markup=keyboard
            )
            return

        data = await db.get_data(user_id)
        if data and data.get("session"):
            session = data.get("session")
            device = 'Vivo Y20'
            userbot = Client(
                ":userbot:",
                api_id=API_ID,
                api_hash=API_HASH,
                device_model=device,
                session_string=session,
                # Add connection settings to improve reliability
                flood_sleep_threshold=SECONDS,  # Sleep on FloodWait up to SECONDS
                retry_delay=1,             # Initial retry delay
                max_retries=5              # Maximum number of retries
            )
            await userbot.start()
        else:
            await app.send_message(message.chat.id, "Login in bot first ...")
            return

        try:
            for i in range(cs, cs + cl):
                if user_id in users_loop and users_loop[user_id]:
                    try:
                        x = start_id.split('/')
                        y = x[:-1]
                        result = '/'.join(y)
                        url = f"{result}/{i}"
                        link = get_link(url)
                        
                        if 't.me/b/' in link or 't.me/c/' in link:
                            msg = await app.send_message(message.chat.id, f"Processing by Crushe...")
                            await safe_execute(process_and_upload_link, userbot, user_id, msg.id, link, 0, message)
                            await pin_msg.edit_text(
                            f"⚡\n__Processing: {i - cs + 1}/{cl}__\n\nBatch process started",
                            reply_markup=keyboard
                            )
                    except Exception as e:
                        print(f"Error processing link {url}: {e}")
                        continue
        finally:
            if userbot.is_connected:
                await userbot.stop()

        await app.send_message(message.chat.id, "Batch completed successfully by Crushe! 🎉")
        await set_interval(user_id, interval_minutes=20)
        await pin_msg.edit_text(
                        f"Batch completed for {cl} messages ⚡\n\n****",
                        reply_markup=keyboard
        )
    except FloodWait as fw:
        wait_time = fw.value if hasattr(fw, 'value') else fw.x
        await app.send_message(
            message.chat.id,
            f"Try again after {wait_time} seconds due to floodwait from Telegram."
        )
        # Sleep and retry automatically
        await asyncio.sleep(wait_time + 1)
        await app.send_message(message.chat.id, "Retrying after FloodWait...")
    except ConnectionError as e:
        await app.send_message(message.chat.id, f"Connection error: {str(e)}. Please try again later.")
    except Timeout as e:
        await app.send_message(message.chat.id, f"Request timed out: {str(e)}. Please try again later.")
    except RequestException as e:
        await app.send_message(message.chat.id, f"Network error: {str(e)}. Please try again later.")
    except Exception as e:
        await app.send_message(message.chat.id, f"Error: {str(e)}")
    finally:
        if 'userbot' in locals() and userbot and userbot.is_connected:
            await userbot.stop()
        users_loop.pop(user_id, None)


@app.on_message(filters.command("cancel"))
async def stop_batch(_, message):
    user_id = message.chat.id
    if user_id in users_loop and users_loop[user_id]:
        users_loop[user_id] = False  
        await app.send_message(
            message.chat.id, 
            "Batch processing has been stopped successfully. You can start a new batch now if you want."
        )
    elif user_id in users_loop and not users_loop[user_id]:
        await app.send_message(
            message.chat.id, 
            "The batch process was already stopped. No active batch to cancel."
        )
    else:
        await app.send_message(
            message.chat.id, 
            "No active batch processing is running to cancel."
        )


# --- Helper and Utility Functions ---

async def check_interval(user_id, freecheck):
    if freecheck != 1 or await is_user_verified(user_id):  
        return True, None

    now = datetime.now()

    if user_id in interval_set:
        cooldown_end = interval_set[user_id]
        if now < cooldown_end:
            remaining_time = (cooldown_end - now).seconds // 60
            return False, f"Please wait {remaining_time} minute(s) before sending another link. Alternatively, purchase premium for instant access.\n\n> Hey 👋 You can use /token to use the bot free for 3 hours without any time limit."
        else:
            del interval_set[user_id]  

    return True, None

async def set_interval(user_id, interval_minutes=5):
    now = datetime.now()
    interval_set[user_id] = now + timedelta(minutes=interval_minutes)

async def process_and_upload_link(userbot, user_id, msg_id, link, retry_count, message):
    try:
        await get_msg(userbot, user_id, msg_id, link, retry_count, message)
        await asyncio.sleep(3.5)
    finally:
        try:
            await app.delete_messages(message.chat.id, msg_id)
        except Exception:
            pass

async def generate_random_name(length=8):
    return ''.join(random.choices(string.ascii_lowercase, k=length))
from crushe import app
from config import API_ID, API_HASH, FREEMIUM_LIMIT, PREMIUM_LIMIT, OWNER_ID
from crushe.core.get_func import get_msg
from crushe.core.func import *
from crushe.core.mongo import db
from crushe.modules.shrink import is_user_verified
from pyrogram.errors import FloodWait
from datetime import datetime, timedelta
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

async def generate_random_name(length=8):
    return ''.join(random.choices(string.ascii_lowercase, k=length))


users_loop = {}
interval_set = {}
batch_mode = {}

async def process_and_upload_link(userbot, user_id, msg_id, link, retry_count, message):
    try:
        # Import error handling utilities
        from crushe.core.error_handler import safe_execute
        # Use safe_execute to handle FloodWait and other errors
        await safe_execute(get_msg, userbot, user_id, msg_id, link, retry_count, message)
        await asyncio.sleep(3.5)
    finally:
        # Auto-delete the "Processing..." message regardless of outcome.
        try:
            await app.delete_messages(message.chat.id, msg_id)
        except Exception:
            pass


async def check_interval(user_id, freecheck):
    if freecheck != 1 or await is_user_verified(user_id):  
        return True, None

    now = datetime.now()
