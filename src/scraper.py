import os
import json
import logging
from datetime import datetime
from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto
from dotenv import load_dotenv
from tqdm import tqdm
from dotenv import load_dotenv
load_dotenv()

# Load environment variables
 

API_ID = os.getenv("TELEGRAM_API_ID")
API_HASH = os.getenv("TELEGRAM_API_HASH")

# Channels to scrape
CHANNELS = {
    "chemed": "https://t.me/chemed",
    "lobelia4cosmetics": "https://t.me/lobelia4cosmetics",
    "tikvahpharma": "https://t.me/tikvahpharma"
}

# Paths
TODAY = datetime.now().strftime("%Y-%m-%d")
RAW_MSG_PATH = f"data/raw/telegram_messages/{TODAY}"
IMAGE_PATH = "data/raw/images"
LOG_PATH = "logs"

os.makedirs(RAW_MSG_PATH, exist_ok=True)
os.makedirs(IMAGE_PATH, exist_ok=True)
os.makedirs(LOG_PATH, exist_ok=True)

# Logging
logging.basicConfig(
    filename=f"{LOG_PATH}/scraper.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

client = TelegramClient("telegram_session", API_ID, API_HASH)

async def scrape_channel(channel_name, channel_url):
    messages_data = []
    image_dir = f"{IMAGE_PATH}/{channel_name}"
    os.makedirs(image_dir, exist_ok=True)

    async for message in client.iter_messages(channel_url, limit=500):
        if not message.text and not message.media:
            continue

        image_file = None
        if isinstance(message.media, MessageMediaPhoto):
            image_file = f"{image_dir}/{message.id}.jpg"
            await client.download_media(message.photo, image_file)

        messages_data.append({
            "message_id": message.id,
            "channel_name": channel_name,
            "message_date": message.date.isoformat() if message.date else None,
            "message_text": message.text,
            "views": message.views,
            "forwards": message.forwards,
            "has_media": bool(message.media),
            "image_path": image_file
        })

    file_path = f"{RAW_MSG_PATH}/{channel_name}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(messages_data, f, ensure_ascii=False, indent=2)

    logging.info(f"Scraped {len(messages_data)} messages from {channel_name}")

async def main():
    await client.start()
    for name, url in CHANNELS.items():
        try:
            logging.info(f"Starting scrape for {name}")
            await scrape_channel(name, url)
        except Exception as e:
            logging.error(f"Error scraping {name}: {e}")
    await client.disconnect()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
print("API_ID:", os.getenv("TELEGRAM_API_ID"))
print("API_HASH:", os.getenv("TELEGRAM_API_HASH"))

