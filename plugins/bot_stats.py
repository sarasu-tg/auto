from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from pyrogram.errors import MessageTooLong
from info import ADMINS, LOG_CHANNEL, USERNAME
from database.users_chats_db import db
from database.ia_filterdb import Media, get_files_db_size
from utils import get_size, temp
from Script import script
from datetime import datetime
import psutil
import time
import os

@Client.on_message(filters.new_chat_members & filters.group)
async def save_group(bot, message):
    check = [u.id for u in message.new_chat_members]
    if temp.ME in check:
        if (str(message.chat.id)).startswith("-100") and not await db.get_chat(message.chat.id):
            total=await bot.get_chat_members_count(message.chat.id)
            user = message.from_user.mention if message.from_user else "Dear" 
            group_link = await message.chat.export_invite_link()
            await bot.send_message(LOG_CHANNEL, script.NEW_GROUP_TXT.format(temp.B_LINK, message.chat.title, message.chat.id, message.chat.username, group_link, total, user), disable_web_page_preview=True)  
            await db.add_chat(message.chat.id, message.chat.title)
            btn = [[
                InlineKeyboardButton('⚡️ sᴜᴘᴘᴏʀᴛ ⚡️', url=USERNAME)
            ]]
            reply_markup=InlineKeyboardMarkup(btn)
            await bot.send_message(
                chat_id=message.chat.id,
                text=f"<b>☤ ᴛʜᴀɴᴋ ʏᴏᴜ ꜰᴏʀ ᴀᴅᴅɪɴɢ ᴍᴇ ɪɴ {message.chat.title}\n\n🤖 ᴅᴏɴ’ᴛ ꜰᴏʀɢᴇᴛ ᴛᴏ ᴍᴀᴋᴇ ᴍᴇ ᴀᴅᴍɪɴ 🤖\n\n㊝ ɪꜰ ʏᴏᴜ ʜᴀᴠᴇ ᴀɴʏ ᴅᴏᴜʙᴛ ʏᴏᴜ ᴄʟᴇᴀʀ ɪᴛ ᴜsɪɴɢ ʙᴇʟᴏᴡ ʙᴜᴛᴛᴏɴs ㊜</b>",
                reply_markup=reply_markup
            )

@Client.on_message(filters.command('leave') & filters.user(ADMINS))
async def leave_a_chat(bot, message):
    r = message.text.split(None)
    if len(message.command) == 1:
        return await message.reply('<b>ᴜꜱᴇ ᴛʜɪꜱ ᴄᴏᴍᴍᴀɴᴅ ʟɪᴋᴇ ᴛʜɪꜱ `/leave -100******`</b>')
    if len(r) > 2:
        reason = message.text.split(None, 2)[2]
        chat = message.text.split(None, 2)[1]
    else:
        chat = message.command[1]
        reason = "ɴᴏ ʀᴇᴀꜱᴏɴ ᴘʀᴏᴠɪᴅᴇᴅ..."
    try:
        chat = int(chat)
    except:
        chat = chat
    try:
        btn = [[
            InlineKeyboardButton('⚡️ ᴏᴡɴᴇʀ ⚡️', url=USERNAME)
        ]]
        reply_markup=InlineKeyboardMarkup(btn)
        await bot.send_message(
            chat_id=chat,
            text=f'😞 ʜᴇʟʟᴏ ᴅᴇᴀʀ,\nᴍʏ ᴏᴡɴᴇʀ ʜᴀꜱ ᴛᴏʟᴅ ᴍᴇ ᴛᴏ ʟᴇᴀᴠᴇ ꜰʀᴏᴍ ɢʀᴏᴜᴘ ꜱᴏ ɪ ɢᴏ 😔\n\n🚫 ʀᴇᴀꜱᴏɴ ɪꜱ - <code>{reason}</code>\n\nɪꜰ ʏᴏᴜ ɴᴇᴇᴅ ᴛᴏ ᴀᴅᴅ ᴍᴇ ᴀɢᴀɪɴ ᴛʜᴇɴ ᴄᴏɴᴛᴀᴄᴛ ᴍʏ ᴏᴡɴᴇʀ 👇',
            reply_markup=reply_markup,
        )
        await bot.leave_chat(chat)
        await db.delete_chat(chat)
        await message.reply(f"<b>ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟʟʏ ʟᴇꜰᴛ ꜰʀᴏᴍ ɢʀᴏᴜᴘ - `{chat}`</b>")
    except Exception as e:
        await message.reply(f'<b>🚫 ᴇʀʀᴏʀ - `{e}`</b>')

@Client.on_message(filters.command('leaveall') & filters.user(ADMINS))
async def leave_all_chats(bot, message):
    msg = await message.reply('<b>Checking all groups...</b>')
    
    chats = await db.get_all_chats()
    left_count = 0
    out = "Groups the bot has left:\n\n"
    count = 1

    async for chat in chats:
        chat_info = await bot.get_chat(chat['id'])
        members_count = chat_info.members_count if chat_info.members_count is not None else 0
        admins = await bot.get_chat_administrators(chat['id'])
        
        bot_added_by_admin = any(admin.user.is_self for admin in admins)

        # Leave if bot was NOT added by an admin AND members are less than 300
        if not bot_added_by_admin and members_count < 300:
            try:
                # Send exit message before leaving
                await bot.send_message(
                    chat['id'], 
                    "I'm leaving this group as per my owner’s instructions.\n"
                    "If you want me to stay, contact @mnbots_support."
                )
            except Exception as e:
                print(f"Failed to send message in {chat['title']}: {e}")

            await bot.leave_chat(chat['id'])
            out += f"<b>{count}. Title - {chat['title']}\nID - {chat['id']}\nMembers - {members_count}</b>\n\n"
            count += 1
            left_count += 1

    if left_count > 0:
        try:
            await msg.edit_text(out)
        except MessageTooLong:
            file_path = 'left_chats.txt'
            with open(file_path, 'w+', encoding='utf-8') as outfile:
                outfile.write(out)
            await message.reply_document(file_path, caption="<b>List of groups the bot left</b>")
            os.remove(file_path)
    else:
        await msg.edit_text("<b>No groups met the leave criteria</b>")

@Client.on_message(filters.command('stats') & filters.user(ADMINS) & filters.incoming)
async def get_ststs(bot, message):
    users = await db.total_users_count()
    groups = await db.total_chat_count()
    size = get_size(await db.get_db_size())
    free = get_size(536870912)
    files = await Media.count_documents()
    db2_size = get_size(await get_files_db_size())
    db2_free = get_size(536870912)
    uptime = time.strftime("%Hh %Mm %Ss", time.gmtime(time.time() - time.time()))
    ram = psutil.virtual_memory().percent
    cpu = psutil.cpu_percent()
    await message.reply_text(script.STATUS_TXT.format(users, groups, size, free, files, db2_size, db2_free, uptime, ram, cpu))
