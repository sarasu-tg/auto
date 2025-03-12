from pyrogram import Client, filters
import datetime
import time
from database.users_chats_db import db
from info import ADMINS
from utils import broadcast_messages
import asyncio

BROADCAST_BATCH_SIZE = 500  # Now processes 500 users/groups at a time
BROADCAST_SLEEP = 1  # Small delay to avoid rate limits

@Client.on_message(filters.command("broadcast") & filters.user(ADMINS) & filters.reply)
async def broadcast(bot, message):
    users = await db.get_all_users()
    groups = await db.get_all_groups()  # Fetch groups from database
    b_msg = message.reply_to_message
    sts = await message.reply_text("Broadcasting your messages...")
    
    start_time = time.time()
    total_users = await db.total_users_count()
    total_groups = await db.total_groups_count()
    total_recipients = total_users + total_groups
    done, blocked, deleted, failed, success = 0, 0, 0, 0, 0

    async def send_message(recipient, is_group=False):
        nonlocal success, blocked, deleted, failed
        chat_id = int(recipient['id'])
        pti, sh = await broadcast_messages(chat_id, b_msg)

        if pti:
            success += 1
        else:
            if sh == "Blocked" and not is_group:
                blocked += 1
                await db.delete_user(chat_id)  # Remove blocked user
            elif sh == "Deleted":
                deleted += 1
                if is_group:
                    await db.delete_group(chat_id)  # Remove deleted group
                else:
                    await db.delete_user(chat_id)  # Remove deleted user
            elif sh == "Error":
                failed += 1

    tasks = []
    async for user in users:
        tasks.append(send_message(user))
        done += 1

        if len(tasks) >= BROADCAST_BATCH_SIZE:
            await asyncio.gather(*tasks)
            tasks = []
            await sts.edit(
                f"Broadcast in progress:\n\nTotal Recipients: {total_recipients}\nCompleted: {done} / {total_recipients}\n"
                f"Success: {success} | Blocked: {blocked} | Deleted: {deleted} | Failed: {failed}"
            )
            await asyncio.sleep(BROADCAST_SLEEP)

    async for group in groups:
        tasks.append(send_message(group, is_group=True))
        done += 1

        if len(tasks) >= BROADCAST_BATCH_SIZE:
            await asyncio.gather(*tasks)
            tasks = []
            await sts.edit(
                f"Broadcast in progress:\n\nTotal Recipients: {total_recipients}\nCompleted: {done} / {total_recipients}\n"
                f"Success: {success} | Blocked: {blocked} | Deleted: {deleted} | Failed: {failed}"
            )
            await asyncio.sleep(BROADCAST_SLEEP)

    if tasks:
        await asyncio.gather(*tasks)

    time_taken = datetime.timedelta(seconds=int(time.time() - start_time))
    await sts.edit(
        f"Broadcast Completed in {time_taken}.\n\nTotal Recipients: {total_recipients}\n"
        f"Success: {success} | Blocked: {blocked} | Deleted: {deleted} | Failed: {failed}"
    )
