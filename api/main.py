from fastapi import FastAPI, HTTPException
from sqlalchemy import text
from api.database import engine
from api.schemas import TopProduct, MessageResult
import nltk
from nltk.corpus import stopwords
from collections import Counter

nltk.download("stopwords")
STOPWORDS = set(stopwords.words("english"))

app = FastAPI(title="Medical Telegram Analytics API")

@app.get("/api/reports/top-products")
def top_products(limit: int = 10):
    with engine.connect() as conn:
        result = conn.execute(text("SELECT message_text FROM fct_messages"))
        words = []
        for row in result:
            for w in row[0].lower().split():
                if w.isalpha() and w not in STOPWORDS:
                    words.append(w)
        counts = Counter(words).most_common(limit)
        return [{"product": k, "count": v} for k, v in counts]


@app.get("/api/channels/{channel_name}/activity")
def channel_activity(channel_name: str):
    with engine.connect() as conn:
        query = text("""
            SELECT date_key, COUNT(*) AS posts, AVG(view_count) AS avg_views
            FROM fct_messages fm
            JOIN dim_channels dc ON fm.channel_key = dc.channel_key
            WHERE dc.channel_name = :channel
            GROUP BY date_key
            ORDER BY date_key
        """)
        rows = conn.execute(query, {"channel": channel_name}).fetchall()

        if not rows:
            raise HTTPException(status_code=404, detail="Channel not found")

        return rows


@app.get("/api/search/messages", response_model=list[MessageResult])
def search_messages(query: str, limit: int = 20):
    with engine.connect() as conn:
        q = text("""
            SELECT message_id, message_text, view_count
            FROM fct_messages
            WHERE message_text ILIKE :q
            LIMIT :limit
        """)
        return conn.execute(q, {"q": f"%{query}%", "limit": limit}).fetchall()


@app.get("/api/reports/visual-content")
def visual_content():
    with engine.connect() as conn:
        q = text("""
            SELECT image_category, COUNT(*) 
            FROM fct_image_detections
            GROUP BY image_category
        """)
        return conn.execute(q).fetchall()
