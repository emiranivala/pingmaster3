import asyncio
import time
import os
import re
import subprocess
import requests
import traceback
from crushe import app
from crushe import sex as gf
from telethon.tl.types import DocumentAttributeVideo
import pymongo
from pyrogram import Client, filters
from pyrogram.errors import ChannelBanned, ChannelInvalid, ChannelPrivate, ChatIdInvalid, ChatInvalid, PeerIdInvalid
from pyrogram.enums import MessageMediaType
from crushe.core.func import progress_bar, video_metadata, screenshot, chk_user, progress_callback, prog_bar
from crushe.core.mongo import db
from crushe.modules.shrink import is_user_verified
from pyrogram.types import Message
from config import MONGO_DB as MONGODB_CONNECTION_STRING, LOG_GROUP, OWNER_ID, STRING, SECONDS
import cv2
import random
from crushe.core.mongo.db import set_session, remove_session, get_data
import string
from telethon import events, Button
from io import BytesIO
from tricky import fast_upload
from crushe.core.connection_manager import ConnectionManager

# ----------------- CHUNK SPLITTING FUNCTIONS -----------------
MAX_CHUNK_SIZE = 2000 * 1024**2  # ~2GB

def split_file(file_path, chunk_size=MAX_CHUNK_SIZE):
    chunk_files = []
    chunk_number = 1
    buffer_size = 64 * 1024  # 64KB
    with open(file_path, "rb") as f:
        while True:
            bytes_written = 0
            chunk_filename = f"{file_path}.part{chunk_number}"
            with open(chunk_filename, "wb") as chunk_file:
                while bytes_written < chunk_size:
                    data = f.read(min(buffer_size, chunk_size - bytes_written))
                    if not data:
                        break
                    chunk_file.write(data)
                    bytes_written += len(data)
            if bytes_written == 0:
                break
            chunk_files.append(chunk_filename)
            chunk_number += 1
    return chunk_files

async def delete_after(message, delay=5):
    await asyncio.sleep(delay)
    try:
        await message.delete()
    except Exception:
        pass
# ---------------------------------------------------------------

async def get_cached_message(cache_key: str):
    """Get a cached message from the ConnectionManager."""
    return await ConnectionManager.get_cached_message(cache_key)

async def cache_message(cache_key: str, message):
    """Cache a message in the ConnectionManager."""
    await ConnectionManager.cache_message(cache_key, message)

def thumbnail(sender):
    return "static/crushe.jpg"

DB_NAME = "smart_users"
COLLECTION_NAME = "super_user"
VIDEO_EXTENSIONS = ['mp4', 'mov', 'avi', 'mkv', 'flv', 'wmv', 'webm', 'mpg', 'mpeg', '3gp', 'ts', 'm4v', 'f4v', 'vob']

mongo_client = pymongo.MongoClient(MONGODB_CONNECTION_STRING)
db = mongo_client[DB_NAME]
collection = db[COLLECTION_NAME]

if STRING:
    from crushe import pro
    print("App imported from crushe.")
else:
    pro = None
    print("STRING is not available. 'app' is set to None.")

async def fetch_upload_method(user_id):
    """Fetch the user's preferred upload method."""
    user_data = collection.find_one({"user_id": user_id})
    return user_data.get("upload_method", "Pyrogram") if user_data else "Pyrogram"

async def get_msg(userbot, sender, edit_id, msg_link, i, message):
    from crushe.core.error_handler import retry_with_backoff
    edit = ""
    chat = ""
    progress_message = None
    round_message = False
    if "?single" in msg_link:
        msg_link = msg_link.split("?single")[0]
    msg_id = int(msg_link.split("/")[-1]) + int(i)
    saved_channel_ids = load_saved_channel_ids()
    if 't.me/c/' in msg_link or 't.me/b/' in msg_link:
        parts = msg_link.split("/")
        if 't.me/b/' not in msg_link:
            chat = int('-100' + str(parts[parts.index('c') + 1]))
        else:
            chat = msg_link.split("/")[-2]
        if chat in saved_channel_ids:
            await app.edit_message_text(message.chat.id, edit_id, "Sorry! dude 😎 This channel is protected 🔐 by **__Crushe__**")
            return
        file = ""
        try:
            size_limit = 2 * 1024 * 1024 * 1024  # 2GB
            chatx = message.chat.id
            
            # Implement retry logic for get_messages which often causes FloodWait
            @retry_with_backoff(max_retries=5, initial_delay=2.0, max_delay=SECONDS)
            async def get_message_with_retry():
                return await userbot.get_messages(chat, msg_id)
            
            # Use ConnectionManager to prevent concurrent read operations
            from crushe.core.connection_manager import ConnectionManager
            
            # Create a unique connection ID based on the userbot session
            connection_id = f"{userbot.session_name}_{chat}"
            
            # Implement message caching to reduce duplicate requests
            cache_key = f"{chat}_{msg_id}"
            cached_msg = await get_cached_message(cache_key)
            if cached_msg:
                msg = cached_msg
                print("Using cached message")
            else:
                # Execute the get_message_with_retry coroutine with a connection lock
                msg = await ConnectionManager.with_connection_lock(connection_id, get_message_with_retry())
                # Cache the message for future use
                await cache_message(cache_key, msg)
            print(msg)
            target_chat_id = user_chat_ids.get(chatx, chatx)
            freecheck = await chk_user(message, sender)
            verified = await is_user_verified(sender)
            original_caption = msg.caption if msg.caption else ''
            custom_caption = get_user_caption_preference(sender)
            final_caption = f"{original_caption}" if custom_caption else f"{original_caption}"
            replacements = load_replacement_words(sender)
            for word, replace_word in replacements.items():
                final_caption = final_caption.replace(word, replace_word)
            caption = f"{final_caption}\n\n__**{custom_caption}**__" if custom_caption else f"{final_caption}"
            if msg.service is not None:
                return None
            if msg.empty is not None:
                return None
            if msg.media:
                if msg.media == MessageMediaType.WEB_PAGE:
                    target_chat_id = user_chat_ids.get(chatx, chatx)
                    edit = await app.edit_message_text(sender, edit_id, "Cloning...")
                    try:
                        message_sent = await app.send_message(target_chat_id, msg.text.markdown)
                        if msg.pinned_message:
                            try:
                                await message_sent.pin(both_sides=True)
                            except Exception as e:
                                await message_sent.pin()
                        await message_sent.copy(LOG_GROUP)
                        await edit.delete()
                        # No file downloaded in this branch—cleanup not needed.
                        return
                    except Exception as e:
                        await edit.edit(f"Error sending message: {str(e)}")
                        return
            if not msg.media:
                if msg.text:
                    target_chat_id = user_chat_ids.get(chatx, chatx)
                    edit = await app.edit_message_text(sender, edit_id, "Cloning...")
                    message_sent = await app.send_message(target_chat_id, msg.text.markdown)
                    if msg.pinned_message:
                        try:
                            await message_sent.pin(both_sides=True)
                        except Exception as e:
                            await message_sent.pin()
                    await message_sent.copy(LOG_GROUP)
                    await edit.delete()
                    return
            if msg.sticker:
                edit = await app.edit_message_text(sender, edit_id, "Sticker detected...")
                result = await app.send_sticker(target_chat_id, msg.sticker.file_id)
                await result.copy(LOG_GROUP)
                await edit.delete(2)
                return
            file_size = None
            if msg.document or msg.photo or msg.video:
                file_size = msg.document.file_size if msg.document else (msg.photo.file_size if msg.photo else msg.video.file_size)
            if file_size and file_size > size_limit and (freecheck == 1 and not verified):
                await edit.edit("**__❌ File size is greater than 2 GB, purchase premium to proceed or use /token to get 3 hour access for free__")
                return
            edit = await app.edit_message_text(sender, edit_id, "Trying to Download...")
            file = await userbot.download_media(
                msg,
                progress=progress_bar,
                progress_args=("╭─────────────────────╮\n│      **__Downloading by Crushe__...**\n├─────────────────────", edit, time.time()))
            # --- Updated File-Renaming Block ---
            custom_rename_tag = get_user_rename_preference(chatx)
            # Detect if the media is truly a video
            is_video = False
            if msg.media == MessageMediaType.VIDEO:
                is_video = True
            elif msg.document and msg.document.mime_type and "video" in msg.document.mime_type.lower():
                is_video = True

            last_dot_index = str(file).rfind('.')
            if last_dot_index != -1 and last_dot_index != 0:
                ggn_ext = str(file)[last_dot_index + 1:]
                if ggn_ext.isalpha() and len(ggn_ext) <= 9:
                    if is_video and ggn_ext.lower() in VIDEO_EXTENSIONS:
                        original_file_name = str(file)[:last_dot_index]
                        file_extension = 'mp4'
                    else:
                        original_file_name = str(file)[:last_dot_index]
                        file_extension = ggn_ext
                else:
                    original_file_name = str(file)
                    file_extension = 'mp4' if is_video else ''
            else:
                original_file_name = str(file)
                file_extension = 'mp4' if is_video else ''

            # Apply delete & replacement words on the filename
            delete_words = load_delete_words(chatx)
            for word in delete_words:
                original_file_name = original_file_name.replace(word, "")
            replacements = load_replacement_words(chatx)
            for word, replace_word in replacements.items():
                original_file_name = original_file_name.replace(word, replace_word)
            if file_extension:
                new_file_name = original_file_name + " " + custom_rename_tag + "." + file_extension
            else:
                new_file_name = original_file_name + " " + custom_rename_tag
            os.rename(file, new_file_name)
            file = new_file_name
            # --- End Updated Block ---

            await edit.edit('Applying Watermark ...')
            metadata = video_metadata(file)
            width = metadata['width']
            height = metadata['height']
            duration = metadata['duration']
            thumb_path = await screenshot(file, duration, chatx)
            file_extension = file.split('.')[-1]
            await edit.edit('**__Checking file...__**')

            # ----------- Updated >2GB Handling (Chunk Splitting) -----------
            file_size = os.path.getsize(file)
            if file_size > 2 * 1024**3:
                try:
                    await edit.delete()
                except Exception:
                    pass
                status_msg1 = await app.send_message(sender, f"Large file detected (> {file_size/1024**3:.2f} GB). Splitting into 2GB chunks...")
                status_msg2 = await app.send_message(sender, "Starting to split the file...")
                chunk_files = split_file(file, MAX_CHUNK_SIZE)
                total_chunks = len(chunk_files)
                status_msg3 = await app.send_message(sender, f"File split into {total_chunks} chunk(s).")
                target_chat_id = user_chat_ids.get(chatx, sender)
                delete_words = load_delete_words(sender)
                custom_caption = get_user_caption_preference(sender)
                original_caption = msg.caption if msg.caption else ''
                final_caption = f"{original_caption}"
                replacements = load_replacement_words(chatx)
                for word, replace_word in replacements.items():
                    final_caption = final_caption.replace(word, replace_word)
                caption = f"{final_caption}\n\n__**{custom_caption}**__" if custom_caption else f"{final_caption}"
                upload_failed = False
                for i, chunk in enumerate(chunk_files):
                    try:
                        chunk_status_msg = await app.send_message(sender, f"Uploading chunk {i+1} of {total_chunks}...")
                        progress_status = await app.send_message(sender, f"Uploading chunk {i+1} of {total_chunks} ...")
                        chunk_caption = caption + f"\n\nPart {i+1} of {total_chunks}"
                        message_sent = await app.send_document(
                            chat_id=target_chat_id,
                            document=chunk,
                            caption=chunk_caption,
                            progress=progress_bar,
                            progress_args=('**Uploading by Crushe...**', progress_status, time.time())
                        )
                        if msg.pinned_message:
                            try:
                                await message_sent.pin(both_sides=True)
                            except Exception:
                                await message_sent.pin()
                        try:
                            await message_sent.copy(LOG_GROUP)
                        except Exception as e:
                            print(f"Error copying chunk to LOG_GROUP: {e}")
                        await app.edit_message_text(sender, progress_status.id, f"Chunk {i+1} of {total_chunks} uploaded successfully!")
                        asyncio.create_task(delete_after(chunk_status_msg))
                        asyncio.create_task(delete_after(progress_status))
                    except Exception as chunk_error:
                        upload_failed = True
                        await app.send_message(sender, f"Error uploading chunk {i+1}: {chunk_error}")
                    finally:
                        if os.path.exists(chunk):
                            os.remove(chunk)
                if os.path.exists(file):
                    os.remove(file)
                file = None
                if not upload_failed:
                    asyncio.create_task(delete_after(status_msg1))
                    asyncio.create_task(delete_after(status_msg2))
                    asyncio.create_task(delete_after(status_msg3))
                    final_status = await app.send_message(sender, "All chunks uploaded successfully!")
                    asyncio.create_task(delete_after(final_status))
                return
            # ----------- End Chunk Splitting Block ---------------------

            if msg.voice:
                result = await app.send_voice(target_chat_id, file)
                await result.copy(LOG_GROUP)
                if os.path.exists(file):
                    os.remove(file)
                file = None
            elif msg.audio:
                result = await app.send_audio(target_chat_id, file, caption=caption)
                await result.copy(LOG_GROUP)
                if os.path.exists(file):
                    os.remove(file)
                file = None
            elif msg.media == MessageMediaType.VIDEO and msg.video.mime_type in ["video/mp4", "video/x-matroska"]:
                metadata = video_metadata(file)
                width = metadata['width']
                height = metadata['height']
                duration = metadata['duration']
                thumb_path = await screenshot(file, duration, chatx)
                if duration <= 300:
                    upload_method = await fetch_upload_method(sender)
                    if upload_method == "Pyrogram":
                        message_sent = await app.send_video(
                            chat_id=target_chat_id,
                            video=file,
                            caption=caption,
                            height=height,
                            width=width,
                            duration=duration,
                            thumb=thumb_path,
                            progress=progress_bar,
                            progress_args=("╭─────────────────────╮\n│      **__Crushe Uploader__**\n├─────────────────────", edit, time.time())
                        )
                        await message_sent.copy(LOG_GROUP)
                        await edit.delete()
                        if os.path.exists(file):
                            os.remove(file)
                        file = None
                        return
                    elif upload_method == "Telethon":
                        await edit.delete()
                        progress_message = await gf.send_message(sender, "__**Uploading by crushe ...**__")
                        uploaded = await fast_upload(
                            gf,
                            file,
                            reply=progress_message,
                            name=None,
                            progress_bar_function=lambda done, total: progress_callback(done, total, sender)
                        )
                        await gf.send_file(
                            target_chat_id,
                            uploaded,
                            caption=caption,
                            attributes=[DocumentAttributeVideo(duration=duration, w=width, h=height, supports_streaming=True)],
                            thumb=thumb_path
                        )
                        await gf.send_file(
                            LOG_GROUP,
                            uploaded,
                            caption=caption,
                            attributes=[DocumentAttributeVideo(duration=duration, w=width, h=height, supports_streaming=True)],
                            thumb=thumb_path
                        )
                        await progress_message.delete()
                        if os.path.exists(file):
                            os.remove(file)
                        file = None
                        return
                delete_words = load_delete_words(sender)
                custom_caption = get_user_caption_preference(sender)
                original_caption = msg.caption if msg.caption else ''
                final_caption = f"{original_caption}" if custom_caption else f"{original_caption}"
                replacements = load_replacement_words(sender)
                for word, replace_word in replacements.items():
                    final_caption = final_caption.replace(word, replace_word)
                caption = f"{final_caption}\n\n__**{custom_caption}**__" if custom_caption else f"{final_caption}"
                target_chat_id = user_chat_ids.get(chatx, chatx)
                thumb_path = await screenshot(file, duration, chatx)
                upload_method = await fetch_upload_method(sender)
                try:
                    if upload_method == "Pyrogram":
                        message_sent = await app.send_video(
                            chat_id=target_chat_id,
                            video=file,
                            caption=caption,
                            supports_streaming=True,
                            height=height,
                            width=width,
                            thumb=thumb_path,
                            duration=duration,
                            progress=progress_bar,
                            progress_args=("╭─────────────────────╮\n│      **__Crushe Uploader__**\n├─────────────────────", edit, time.time())
                        )
                        await message_sent.copy(LOG_GROUP)
                    elif upload_method == "Telethon":
                        await edit.delete()
                        progress_message = await gf.send_message(sender, "**__Starting Upload by Crushe__**")
                        uploaded = await fast_upload(
                            gf,
                            file,
                            reply=progress_message,
                            name=None,
                            progress_bar_function=lambda done, total: progress_callback(done, total, sender)
                        )
                        await gf.send_file(
                            target_chat_id,
                            uploaded,
                            caption=caption,
                            attributes=[DocumentAttributeVideo(duration=duration, w=width, h=height, supports_streaming=True)],
                            thumb=thumb_path
                        )
                        await gf.send_file(
                            LOG_GROUP,
                            uploaded,
                            caption=caption,
                            attributes=[DocumentAttributeVideo(duration=duration, w=width, h=height, supports_streaming=True)],
                            thumb=thumb_path
                        )
                    # In either branch, after upload, remove the file.
                    if os.path.exists(file):
                        os.remove(file)
                    file = None
                except Exception:
                    try:
                        await app.edit_message_text(sender, edit_id, "The bot is not an admin in the specified chat.")
                    except Exception:
                        await progress_message.edit("Something Greate happened my jaan")
            elif msg.media == MessageMediaType.PHOTO:
                await edit.edit("**Uploading photo...")
                delete_words = load_delete_words(sender)
                custom_caption = get_user_caption_preference(sender)
                original_caption = msg.caption if msg.caption else ''
                final_caption = f"{original_caption}\n\n__**{custom_caption}**__" if custom_caption else f"{original_caption}"
                replacements = load_replacement_words(sender)
                for word, replace_word in replacements.items():
                    final_caption = final_caption.replace(word, replace_word)
                caption = final_caption
                target_chat_id = user_chat_ids.get(sender, sender)
                message_sent = await app.send_photo(chat_id=target_chat_id, photo=file, caption=caption)
                if msg.pinned_message:
                    try:
                        await message_sent.pin(both_sides=True)
                    except Exception as e:
                        await message_sent.pin()
                await message_sent.copy(LOG_GROUP)
                if os.path.exists(file):
                    os.remove(file)
                file = None
            else:
                delete_words = load_delete_words(sender)
                custom_caption = get_user_caption_preference(sender)
                original_caption = msg.caption if msg.caption else ''
                final_caption = f"{original_caption}" if custom_caption else f"{original_caption}"
                replacements = load_replacement_words(chatx)
                for word, replace_word in replacements.items():
                    final_caption = final_caption.replace(word, replace_word)
                caption = f"{final_caption}\n\n__**{custom_caption}**__" if custom_caption else f"{final_caption}"
                file_extension = file_extension.lower()
                video_extensions = {'mkv', 'mp4', 'webm', 'mpe4', 'mpeg', 'ts', 'avi', 'flv', 'mov', 'm4v', '3gp', '3g2', 'wmv', 'vob', 'ogv', 'ogx', 'qt', 'f4v', 'f4p', 'f4a', 'f4b', 'dat', 'rm', 'rmvb', 'asf', 'amv', 'divx'}
                target_chat_id = user_chat_ids.get(chatx, chatx)
                upload_method = await fetch_upload_method(sender)
                try:
                    if file_extension in video_extensions:
                        if upload_method == "Pyrogram":
                            message_sent = await app.send_video(
                                chat_id=target_chat_id,
                                video=file,
                                caption=caption,
                                supports_streaming=True,
                                height=height,
                                width=width,
                                duration=duration,
                                thumb=thumb_path,
                                progress=progress_bar,
                                progress_args=("╭─────────────────────╮\n│      **__Crushe Uploader__**\n├─────────────────────", edit, time.time())
                            )
                            await message_sent.copy(LOG_GROUP)
                        elif upload_method == "Telethon":
                            await edit.delete()
                            progress_message = await gf.send_message(sender, "**__Starting Upload by Crushe__**")
                            uploaded = await fast_upload(
                                gf,
                                file,
                                reply=progress_message,
                                name=None,
                                progress_bar_function=lambda done, total: progress_callback(done, total, sender)
                            )
                            await gf.send_file(
                                target_chat_id,
                                uploaded,
                                caption=caption,
                                attributes=[DocumentAttributeVideo(duration=metadata['duration'], w=metadata['width'], h=metadata['height'], supports_streaming=True)],
                                thumb=thumb_path
                            )
                            await gf.send_file(
                                LOG_GROUP,
                                uploaded,
                                caption=caption,
                                attributes=[DocumentAttributeVideo(duration=metadata['duration'], w=metadata['width'], h=metadata['height'], supports_streaming=True)],
                                thumb=thumb_path
                            )
                    else:
                        if upload_method == "Pyrogram":
                            message_sent = await app.send_document(
                                chat_id=target_chat_id,
                                document=file,
                                caption=caption,
                                thumb=thumb_path,
                                progress=progress_bar,
                                progress_args=("╭─────────────────────╮\n│      **__Crushe Uploader__**\n├─────────────────────", edit, time.time())
                            )
                            await message_sent.copy(LOG_GROUP)
                        elif upload_method == "Telethon":
                            await edit.delete()
                            progress_message = await gf.send_message(sender, "Uploading by Crushe...")
                            uploaded = await fast_upload(
                                gf,
                                file,
                                reply=progress_message,
                                name=None,
                                progress_bar_function=lambda done, total: progress_callback(done, total, sender)
                            )
                            await gf.send_file(target_chat_id, uploaded, caption=caption, thumb=thumb_path)
                            await gf.send_file(LOG_GROUP, uploaded, caption=caption, thumb=thumb_path)
                    if os.path.exists(file):
                        os.remove(file)
                    file = None
                except Exception:
                    try:
                        await app.edit_message_text(sender, edit_id, "The bot is not an admin in the specified chat.")
                    except Exception:
                        await progress_message.edit("Something Greate happened my jaan")
            await edit.delete()
            if progress_message:
                await progress_message.delete()
            return
        except (ChannelBanned, ChannelInvalid, ChannelPrivate, ChatIdInvalid, ChatInvalid):
            await app.edit_message_text(sender, edit_id, "Have you joined the channel?")
            return
        except Exception as e:
            print(f"Errrrror {e}")
            await edit.delete()
    else:
        edit = await app.edit_message_text(sender, edit_id, "Cloning by Crushe...")
        try:
            chat = msg_link.split("/")[-2]
            await copy_message_with_chat_id(app, sender, chat, msg_id)
            await edit.delete()
        except Exception as e:
            await app.edit_message_text(sender, edit_id, f'Failed to save: `{msg_link}`\n\nError: {str(e)}')

async def copy_message_with_chat_id(client, sender, chat_id, message_id):
    target_chat_id = user_chat_ids.get(sender, sender)
    try:
        msg = await client.get_messages(chat_id, message_id)
        custom_caption = get_user_caption_preference(sender)
        original_caption = msg.caption if msg.caption else ''
        final_caption = f"{original_caption}" if custom_caption else f"{original_caption}"
        delete_words = load_delete_words(sender)
        for word in delete_words:
            final_caption = final_caption.replace(word, '  ')
        replacements = load_replacement_words(sender)
        for word, replace_word in replacements.items():
            final_caption = final_caption.replace(word, replace_word)
        caption = f"{final_caption}\n\n__**{custom_caption}**__" if custom_caption else f"{final_caption}"
        if msg.media:
            if msg.media == MessageMediaType.VIDEO:
                result = await client.send_video(target_chat_id, msg.video.file_id, caption=caption)
            elif msg.media == MessageMediaType.DOCUMENT:
                result = await client.send_document(target_chat_id, msg.document.file_id, caption=caption)
            elif msg.media == MessageMediaType.PHOTO:
                result = await client.send_photo(target_chat_id, msg.photo.file_id, caption=caption)
            else:
                result = await client.copy_message(target_chat_id, chat_id, message_id)
        else:
            result = await client.copy_message(target_chat_id, chat_id, message_id)
        try:
            await result.copy(LOG_GROUP)
        except Exception:
            pass
        if msg.pinned_message:
            try:
                await result.pin(both_sides=True)
            except Exception as e:
                await result.pin()
    except Exception as e:
        error_message = f"Error occurred while sending message to chat ID {target_chat_id}: {str(e)}"
        await client.send_message(sender, error_message)
        await client.send_message(sender, f"Make Bot admin in your Channel - {target_chat_id} and restart the process after /cancel")

user_states = {}
user_chat_ids = {}

def load_delete_words(user_id):
    try:
        words_data = collection.find_one({"_id": user_id})
        if words_data:
            return set(words_data.get("delete_words", []))
        else:
            return set()
    except Exception as e:
        print(f"Error loading delete words: {e}")
        return set()

def save_delete_words(user_id, delete_words):
    try:
        collection.update_one({"_id": user_id}, {"$set": {"delete_words": list(delete_words)}}, upsert=True)
    except Exception as e:
        print(f"Error saving delete words: {e}")

def load_replacement_words(user_id):
    try:
        words_data = collection.find_one({"_id": user_id})
        if words_data:
            return words_data.get("replacement_words", {})
        else:
            return {}
    except Exception as e:
        print(f"Error loading replacement words: {e}")
        return {}

def save_replacement_words(user_id, replacements):
    try:
        collection.update_one({"_id": user_id}, {"$set": {"replacement_words": replacements}}, upsert=True)
    except Exception as e:
        print(f"Error saving replacement words: {e}")

user_rename_preferences = {}
user_caption_preferences = {}

def load_user_session(sender_id):
    user_data = collection.find_one({"user_id": sender_id})
    if user_data:
        return user_data.get("session")
    else:
        return None

async def set_rename_command(user_id, custom_rename_tag):
    user_rename_preferences[str(user_id)] = custom_rename_tag

def get_user_rename_preference(user_id):
    return user_rename_preferences.get(str(user_id), ' ')

async def set_caption_command(user_id, custom_caption):
    user_caption_preferences[str(user_id)] = custom_caption

def get_user_caption_preference(user_id):
    return user_caption_preferences.get(str(user_id), '')

sessions = {}
SET_PIC = "settings.jpg"
MESS = "Customize by your end and Configure your settings ..."

@gf.on(events.NewMessage(incoming=True, pattern='/settings'))
async def settings_command(event):
    buttons = [
        [Button.inline("Set Chat ID", b'setchat'), Button.inline("Set Rename Tag", b'setrename')],
        [Button.inline("Caption", b'setcaption'), Button.inline("Replace Words", b'setreplacement')],
        [Button.inline("Remove Words", b'delete'), Button.inline("Reset", b'reset')],
        [Button.inline("Session Login", b'addsession'), Button.inline("Logout", b'logout')],
        [Button.inline("Set Thumbnail", b'setthumb'), Button.inline("Remove Thumbnail", b'remthumb')],
        [Button.inline("Upload Method", b'uploadmethod')],
        [Button.url("Report Errors", "https://t.me/She_who_remain")]
    ]
    await gf.send_file(event.chat_id, file=SET_PIC, caption=MESS, buttons=buttons)

pending_photos = {}

@gf.on(events.CallbackQuery)
async def callback_query_handler(event):
    user_id = event.sender_id
    if event.data == b'setchat':
        await event.respond("Send me the ID of that chat:")
        sessions[user_id] = 'setchat'
    elif event.data == b'setrename':
        await event.respond("Send me the rename tag:")
        sessions[user_id] = 'setrename'
    elif event.data == b'setcaption':
        await event.respond("Send me the caption:")
        sessions[user_id] = 'setcaption'
    elif event.data == b'setreplacement':
        await event.respond("Send me the replacement words in the format: 'WORD(s)' 'REPLACEWORD'")
        sessions[user_id] = 'setreplacement'
    elif event.data == b'addsession':
        await event.respond("Send Pyrogram V2 session")
        sessions[user_id] = 'addsession'
    elif event.data == b'delete':
        await event.respond("Send words seperated by space to delete them from caption/filename ...")
        sessions[user_id] = 'deleteword'
    elif event.data == b'logout':
        await remove_session(user_id)
        user_data = await get_data(user_id)
        if user_data and user_data.get("session") is None:
            await event.respond("Logged out and deleted session successfully.")
        else:
            await event.respond("You are not logged in.")
    elif event.data == b'setthumb':
        pending_photos[user_id] = True
        await event.respond('Please send the photo you want to set as the thumbnail.')
    elif event.data == b'uploadmethod':
        user_data = collection.find_one({'user_id': user_id})
        current_method = user_data.get('upload_method', 'Pyrogram') if user_data else 'Pyrogram'
        pyrogram_check = " ✅" if current_method == "Pyrogram" else ""
        telethon_check = " ✅" if current_method == "Telethon" else ""
        buttons = [
            [Button.inline(f"Pyrogram v2{pyrogram_check}", b'pyrogram')],
            [Button.inline(f"Crushe v1 ⚡{telethon_check}", b'telethon')]
        ]
        await event.edit("Choose your preferred upload method:\n\n__**Note:** **Crushe ⚡**, built on Telethon(base), still in beta.__", buttons=buttons)
    elif event.data == b'pyrogram':
        save_user_upload_method(user_id, "Pyrogram")
        await event.edit("Upload method set to **Pyrogram** ✅")
    elif event.data == b'telethon':
        save_user_upload_method(user_id, "Telethon")
        await event.edit("Upload method set to **Crushe ⚡\n\nThanks for choosing this library as it will help me to analyze the error raise issues on github.** ✅")
    elif event.data == b'reset':
        try:
            user_id_str = str(user_id)
            collection.update_one({"_id": user_id}, {"$unset": {"delete_words": "", "replacement_words": "", "watermark_text": "", "duration_limit": ""}})
            collection.update_one({"user_id": user_id}, {"$unset": {"delete_words": "", "replacement_words": "", "watermark_text": "", "duration_limit": ""}})
            user_chat_ids.pop(user_id, None)
            user_rename_preferences.pop(user_id_str, None)
            user_caption_preferences.pop(user_id_str, None)
            thumbnail_path = f"{user_id}.jpg"
            if os.path.exists(thumbnail_path):
                os.remove(thumbnail_path)
            await event.respond("✅ Reset successfully, to logout click /logout")
        except Exception as e:
            await event.respond(f"Error clearing delete list: {e}")
    elif event.data == b'remthumb':
        try:
            os.remove(f'{user_id}.jpg')
            await event.respond('Thumbnail removed successfully!')
        except FileNotFoundError:
            await event.respond("No thumbnail found to remove.")

@gf.on(events.NewMessage(func=lambda e: e.sender_id in pending_photos))
async def save_thumbnail(event):
    user_id = event.sender_id
    if event.photo:
        temp_path = await event.download_media()
        await event.respond('Using default thumbnail from static/crushe.jpg')
    pending_photos.pop(user_id, None)

@gf.on(events.NewMessage)
async def handle_user_input(event):
    user_id = event.sender_id
    if user_id in sessions:
        session_type = sessions[user_id]
        if session_type == 'setchat':
            try:
                chat_id = int(event.text)
                user_chat_ids[user_id] = chat_id
                await event.respond("Chat ID set successfully!")
            except ValueError:
                await event.respond("Invalid chat ID!")
        elif session_type == 'setrename':
            custom_rename_tag = event.text
            await set_rename_command(user_id, custom_rename_tag)
            await event.respond(f"Custom rename tag set to: {custom_rename_tag}")
        elif session_type == 'setcaption':
            custom_caption = event.text
            await set_caption_command(user_id, custom_caption)
            await event.respond(f"Custom caption set to: {custom_caption}")
        elif session_type == 'setreplacement':
            match = re.match(r"'(.+)' '(.+)'", event.text)
            if not match:
                await event.respond("Usage: 'WORD(s)' 'REPLACEWORD'")
            else:
                word, replace_word = match.groups()
                delete_words = load_delete_words(user_id)
                if word in delete_words:
                    await event.respond(f"The word '{word}' is in the delete set and cannot be replaced.")
                else:
                    replacements = load_replacement_words(user_id)
                    replacements[word] = replace_word
                    save_replacement_words(user_id, replacements)
                    await event.respond(f"Replacement saved: '{word}' will be replaced with '{replace_word}'")
        elif session_type == 'addsession':
            session_data = {
                "user_id": user_id,
                "session_string": event.text
            }
            mcollection.update_one(
                {"user_id": user_id},
                {"$set": session_data},
                upsert=True
            )
            await event.respond("Session string added successfully.")
        elif session_type == 'deleteword':
            words_to_delete = event.message.text.split()
            delete_words = load_delete_words(user_id)
            delete_words.update(words_to_delete)
            save_delete_words(user_id, delete_words)
            await event.respond(f"Words added to delete list: {', '.join(words_to_delete)}")
        del sessions[user_id]

def load_saved_channel_ids():
    saved_channel_ids = set()
    try:
        for channel_doc in collection.find({"channel_id": {"$exists": True}}):
            saved_channel_ids.add(channel_doc["channel_id"])
    except Exception as e:
        print(f"Error loading saved channel IDs: {e}")
    return saved_channel_ids

@gf.on(events.NewMessage(incoming=True, pattern='/lock'))
async def lock_command_handler(event):
    if event.sender_id not in OWNER_ID:
        return await event.respond("You are not authorized to use this command.")
    try:
        channel_id = int(event.text.split(' ')[1])
    except (ValueError, IndexError):
        return await event.respond("Invalid /lock command. Use /lock CHANNEL_ID.")
    try:
        collection.insert_one({"channel_id": channel_id})
        await event.respond(f"Channel ID {channel_id} locked successfully.")
    except Exception as e:
        await event.respond(f"Error occurred while locking channel ID: {str(e)}")

user_progress = {}

def progress_callback(done, total, user_id):
    if user_id not in user_progress:
        user_progress[user_id] = {'previous_done': 0, 'previous_time': time.time()}
    user_data = user_progress[user_id]
    percent = (done / total) * 100
    completed_blocks = int(percent // 10)
    fractional_block = int((percent % 10) // 1)
    remaining_blocks = 10 - completed_blocks - (1 if fractional_block > 0 else 0)
    progress_bar_str = "✅" * completed_blocks
    if fractional_block > 0:
        progress_bar_str += "🟨"
    progress_bar_str += "🟥" * remaining_blocks
    done_mb = done / (1024 * 1024)
    total_mb = total / (1024 * 1024)
    speed = done - user_data['previous_done']
    elapsed_time = time.time() - user_data['previous_time']
    if elapsed_time > 0:
        speed_bps = speed / elapsed_time
        speed_mbps = (speed_bps * 8) / (1024 * 1024)
    else:
        speed_mbps = 0
    if speed_bps > 0:
        remaining_time = (total - done) / speed_bps
    else:
        remaining_time = 0
    remaining_time_min = remaining_time / 60
    final = (f"╭──────────────────╮\n"
             f"│     **__Crushe ⚡ Uploader__**       \n"
             f"├──────────\n"
             f"│ {progress_bar_str}\n\n"
             f"│ **__Progress:__** {percent:.2f}%\n"
             f"│ **__Done:__** {done_mb:.2f} MB / {total_mb:.2f} MB\n"
             f"│ **__Speed:__** {speed_mbps:.2f} Mbps\n"
             f"│ **__ETA:__** {remaining_time_min:.2f} min\n"
             f"╰──────────────────╯\n\n"
             f"**__Powered by Crushe__**")
    user_data['previous_done'] = done
    user_data['previous_time'] = time.time()
    return final

async def add_pdf_watermark(input_pdf, output_pdf_path, watermark_text):
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, add_pdf_watermark_sync, input_pdf, output_pdf_path, watermark_text)
    return result

# Code completed
