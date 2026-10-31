# Boti: Telegram Message Scheduler

![Python](https://img.shields.io/badge/Python-3-3776AB?logo=python&logoColor=white)
![Telethon](https://img.shields.io/badge/Telethon-1.36-26A5E4?logo=telegram&logoColor=white)

A Telegram bot + user client that schedules messages to your chats.

## Features

- `/start` or `/menu` opens a button menu
- "📝 Schedule" flow: send `message | time`-style input to queue a message
- Scheduled messages stored in SQLite (`messages.db`)
- Flood-wait handling

## Built with

- **Language:** Python 3
- **Libraries:** [Telethon](https://github.com/LonamiWebs/Telethon), python-dotenv
- **Storage:** SQLite

## Getting started

```bash
git clone https://github.com/ashswin-git/Boti.git
cd Boti
pip install -r requirements.txt
python bott.py
```

> **Security note:** set your own Telegram API credentials and bot token before running, and keep them out of the repository.
