import asyncio
import logging
import random
import sqlite3
from datetime import datetime, timedelta, timezone

from telethon import TelegramClient, events, Button
from telethon.errors import FloodWaitError
from telethon.tl.types import Dialog

# ────────────────────────────────────────────────
# YOUR CREDENTIALS
# ────────────────────────────────────────────────

API_ID = 35547110
API_HASH = "47296bf904ea7b45ffc0a71495715ed0"
PHONE = "+919430726027"
BOT_TOKEN = "7883451529:AAGikzdh6yZagJp5zoVkA2j3Wj-HJY2ShN8"

DB_FILE = "messages.db"
SESSION_USER = "user_session"
SESSION_BOT = "bot_session"

# ────────────────────────────────────────────────
# LOGGING
# ────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger(__name__)

# ────────────────────────────────────────────────
# DATABASE
# ────────────────────────────────────────────────

conn = sqlite3.connect(DB_FILE, check_same_thread=False)
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS scheduled_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message TEXT NOT NULL,
    schedule_type TEXT NOT NULL,
    times TEXT,
    next_run TEXT NOT NULL,
    interval_sec INTEGER,
    group_ids TEXT,
    is_active INTEGER DEFAULT 1
)
''')
conn.commit()

# ────────────────────────────────────────────────
# CLIENTS
# ────────────────────────────────────────────────

user_client = TelegramClient(SESSION_USER, API_ID, API_HASH)
bot_client = TelegramClient(SESSION_BOT, API_ID, API_HASH)

groups = []

# ────────────────────────────────────────────────
# HELPERS
# ────────────────────────────────────────────────

def now_utc():
    return datetime.now(timezone.utc)

def parse_times(time_str):
    if not time_str: return None
    parts = [p.strip() for p in time_str.split(',') if p.strip()]
    results = []
    now = now_utc()
    for part in parts:
        try:
            t = datetime.strptime(part, "%H:%M").time()
            dt = now.replace(hour=t.hour, minute=t.minute, second=0, microsecond=0)
            if dt <= now: dt += timedelta(days=1)
            results.append(dt)
        except:
            continue
    return results if results else None

async def send_to_all_groups(message):
    total = len(groups)
    success = 0
    for g in groups:
        try:
            await user_client.send_message(g.id, message)
            success += 1
            await asyncio.sleep(random.uniform(4, 8))
        except FloodWaitError as e:
            await asyncio.sleep(e.seconds + 10)
        except Exception as e:
            logger.error(f"Send failed: {e}")
    return success, total

def save_schedule(message, sched_type, times_str=""):
    group_ids = ",".join(str(g.id) for g in groups) if groups else ""
    next_run = None
    interval = None

    if sched_type == "scheduleonce":
        times = parse_times(times_str)
        if not times: return None
        times.sort()
        next_run = times[0]
        times_str = ",".join(t.strftime("%H:%M") for t in times)

    elif sched_type in ("daily", "custom"):
        times = parse_times(times_str)
        if not times: return None
        next_run = times[0]

    elif sched_type == "hourly":
        next_run = now_utc().replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        interval = 3600

    elif sched_type == "every5min":
        next_run = now_utc() + timedelta(minutes=5)
        interval = 300

    elif sched_type == "every15min":
        next_run = now_utc() + timedelta(minutes=15)
        interval = 900

    elif sched_type == "every30min":
        next_run = now_utc() + timedelta(minutes=30)
        interval = 1800

    if next_run is None:
        return None

    cursor.execute("""
        INSERT INTO scheduled_messages
        (message, schedule_type, times, next_run, interval_sec, group_ids)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (message, sched_type, times_str, next_run.isoformat(), interval, group_ids))
    conn.commit()
    return cursor.lastrowid

def update_next_run(sid, new_dt):
    cursor.execute("UPDATE scheduled_messages SET next_run = ? WHERE id = ?", (new_dt.isoformat(), sid))
    conn.commit()

def deactivate_schedule(sid):
    cursor.execute("UPDATE scheduled_messages SET is_active = 0 WHERE id = ?", (sid,))
    conn.commit()

async def process_schedules():
    while True:
        await asyncio.sleep(30)
        now = now_utc()
        cursor.execute("SELECT * FROM scheduled_messages WHERE is_active=1 AND next_run <= ?", (now.isoformat(),))
        for row in cursor.fetchall():
            sid, msg, typ, times_str, next_run, interval, _ = row
            await send_to_all_groups(msg)

            if typ == "scheduleonce":
                times = parse_times(times_str)
                if times:
                    remaining = [t for t in times if t > now]
                    if remaining:
                        remaining.sort()
                        update_next_run(sid, remaining[0])
                        continue
                deactivate_schedule(sid)

            elif typ in ("daily", "custom"):
                base = parse_times(times_str)
                if base:
                    next_dt = base[0] + timedelta(days=1)
                    while next_dt <= now: next_dt += timedelta(days=1)
                    update_next_run(sid, next_dt)

            elif interval:
                next_dt = now + timedelta(seconds=interval)
                update_next_run(sid, next_dt)

# ────────────────────────────────────────────────
# KEYBOARD
# ────────────────────────────────────────────────

def get_keyboard():
    return [
        [Button.text("📝 Schedule"), Button.text("📋 List")],
        [Button.text("🗑️ Delete"), Button.text("🔄 Groups")],
        [Button.text("📤 Send Now"), Button.text("❓ Help")]
    ]

# ────────────────────────────────────────────────
# HANDLERS
# ────────────────────────────────────────────────

@bot_client.on(events.NewMessage(pattern=r'^/(start|menu)'))
async def start(event):
    await event.reply(
        "✨ **Mass Sender Bot** ✨\nUse buttons below:",
        buttons=get_keyboard()
    )

@bot_client.on(events.NewMessage(func=lambda e: e.text == "📝 Schedule"))
async def btn_schedule(event):
    await event.reply(
        "Format:\n`message | type [times]`\n\nExamples:\n"
        "• `Hello | daily`\n"
        "• `Reminder | every5min`\n"
        "• `Update | every30min`\n"
        "• `Meeting | custom 14:30`",
        buttons=get_keyboard()
    )

@bot_client.on(events.NewMessage(func=lambda e: '|' in e.text))
async def schedule_msg(event):
    try:
        msg, rest = [x.strip() for x in event.text.split('|', 1)]
        parts = rest.split(maxsplit=1)
        typ = parts[0].lower()
        times = parts[1].strip() if len(parts) > 1 else ""

        allowed = {"daily", "hourly", "every5min", "every15min", "every30min", "custom", "scheduleonce"}
        if typ not in allowed:
            await event.reply(f"Allowed: {', '.join(allowed)}", buttons=get_keyboard())
            return

        if typ in ("custom", "scheduleonce") and not times:
            await event.reply("Add time(s)", buttons=get_keyboard())
            return

        sid = save_schedule(msg, typ, times)
        if sid is None:
            await event.reply("Invalid time format", buttons=get_keyboard())
            return

        reply = f"Scheduled #{sid}\nType: {typ}"
        if times: reply += f"\nTimes: {times}"
        await event.reply(reply, buttons=get_keyboard())
    except:
        await event.reply("Error – check format", buttons=get_keyboard())

# Add List, Delete, Send Now, Groups, Help handlers from your previous version if needed

# ────────────────────────────────────────────────
# MAIN
# ────────────────────────────────────────────────

async def main():
    await user_client.start(phone=PHONE)
    await bot_client.start(bot_token=BOT_TOKEN)

    global groups
    dialogs = await user_client.get_dialogs()
    groups = [d for d in dialogs if d.is_group or d.is_channel]

    asyncio.create_task(process_schedules())

    await asyncio.gather(
        user_client.run_until_disconnected(),
        bot_client.run_until_disconnected()
    )

if __name__ == '__main__':
    asyncio.run(main())
