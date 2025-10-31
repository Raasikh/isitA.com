from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import feedparser
import psycopg2
from email.utils import parsedate_to_datetime
import logging

logger = logging.getLogger(__name__)

RSS_FEEDS_CONFIG = RSS_FEEDS_CONFIG = [
    # Government/Official Sources
    {
        "name": "cisa",
        "url": "https://www.cisa.gov/news.xml",
        "category": "cybersecurity_government",
        "primary_domain": "Government/CISA",
        "bias_expected": "center",
        "fetch_frequency_hours": 1
    },
    {
        "name": "nist",
        "url": "https://www.nist.gov/news-events/resilience/rss.xml",
        "category": "cybersecurity_standards",
        "primary_domain": "Standards/NIST",
        "bias_expected": "center",
        "fetch_frequency_hours": 6
    },
    
    # Major News Outlets - Tech Coverage
    {
        "name": "bbc_tech",
        "url": "https://feeds.bbc.co.uk/news/technology/rss.xml",
        "category": "tech_news_mainstream",
        "primary_domain": "News/BBC",
        "bias_expected": "center",
        "fetch_frequency_hours": 2
    },
    {
        "name": "reuters_tech",
        "url": "https://www.reuters.com/rssFeed/technologyNews",
        "category": "tech_news_mainstream",
        "primary_domain": "News/Reuters",
        "bias_expected": "center",
        "fetch_frequency_hours": 2
    },
    {
        "name": "apnews_tech",
        "url": "https://apnews.com/hub/technology/feed",
        "category": "tech_news_mainstream",
        "primary_domain": "News/AP",
        "bias_expected": "center",
        "fetch_frequency_hours": 2
    },
    
    # Cybersecurity Specialized
    {
        "name": "bleepingcomputer",
        "url": "https://www.bleepingcomputer.com/feed/",
        "category": "cybersecurity_specialist",
        "primary_domain": "Specialist/BleepingComputer",
        "bias_expected": "center",
        "fetch_frequency_hours": 1
    },
    {
        "name": "darkreading",
        "url": "https://www.darkreading.com/feed/",
        "category": "cybersecurity_specialist",
        "primary_domain": "Specialist/DarkReading",
        "bias_expected": "center-right",
        "fetch_frequency_hours": 2
    },
    {
        "name": "securityweek",
        "url": "https://feeds.securityweek.com/securityweek",
        "category": "cybersecurity_specialist",
        "primary_domain": "Specialist/SecurityWeek",
        "bias_expected": "center",
        "fetch_frequency_hours": 2
    },
    
    # Tech Industry
    {
        "name": "techcrunch",
        "url": "https://techcrunch.com/feed/",
        "category": "tech_industry",
        "primary_domain": "Industry/TechCrunch",
        "bias_expected": "center-left",
        "fetch_frequency_hours": 3
    },
    {
        "name": "theverge",
        "url": "https://www.theverge.com/rss/index.xml",
        "category": "tech_industry",
        "primary_domain": "Industry/TheVerge",
        "bias_expected": "center",
        "fetch_frequency_hours": 3
    },
    {
        "name": "arstechnica",
        "url": "https://arstechnica.com/feed/",
        "category": "tech_industry",
        "primary_domain": "Industry/ArsTechnica",
        "bias_expected": "center",
        "fetch_frequency_hours": 2
    },
    
    # Emerging/Tech-Forward
    {
        "name": "wired_security",
        "url": "https://www.wired.com/feed/rss",
        "category": "tech_industry",
        "primary_domain": "Industry/Wired",
        "bias_expected": "center-left",
        "fetch_frequency_hours": 3
    },
    {
        "name": "zdnet",
        "url": "https://www.zdnet.com/feed/rss.xml",
        "category": "tech_news_mainstream",
        "primary_domain": "News/ZDNet",
        "bias_expected": "center",
        "fetch_frequency_hours": 2
    },
]

# Organization by coverage type:
# 1. Government Sources (CISA, NIST) - Official
# 2. Mainstream News (BBC, Reuters, AP) - Neutral
# 3. Cybersecurity Specialists (Bleeping Computer, Dark Reading, SecurityWeek)
# 4. Tech Industry (TechCrunch, The Verge, Ars Technica, Wired) - Tech-focused
# 5. Tech General (ZDNet) - Cross-domain

PG_CONN = {
    "host": "host.docker.internal",
    "database": "app_db",
    "user": "postgres",
    "password": "postgres",
    "port": 5432
}

def fetch_and_store_rss():
    try:
        conn = psycopg2.connect(**PG_CONN)
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS news_articles (
                id SERIAL PRIMARY KEY,
                source VARCHAR(50),
                title TEXT,
                link TEXT UNIQUE,
                published TIMESTAMP,
                category VARCHAR(100),
                domain VARCHAR(100),
                source_type VARCHAR(20) DEFAULT 'rss',
                bias_expected VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        logger.info("Table created/verified")

        articles_inserted = 0

        for feed in RSS_FEEDS_CONFIG:
            logger.info(f"Fetching RSS feed: {feed['name']}")
            try:
                data = feedparser.parse(feed["url"])

                if data.bozo:
                    logger.warning(f"Feed parsing warning for {feed['name']}: {data.bozo_exception}")

                for entry in data.entries:
                    title = entry.get("title", "No Title")
                    link = entry.get("link", "No Link")

                    published = None
                    if hasattr(entry, "published"):
                        try:
                            published = parsedate_to_datetime(entry.published)
                        except Exception as e:
                            logger.warning(f"Could not parse date for {title}: {e}")

                    try:
                        cur.execute(
                            """
                            INSERT INTO news_articles 
                            (source, title, link, published, category, domain, source_type, bias_expected)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s) 
                            ON CONFLICT (link) DO NOTHING;
                            """,
                            (
                                feed["name"],
                                title,
                                link,
                                published,
                                feed["category"],
                                feed["primary_domain"],
                                'rss',  # source_type
                                feed.get("bias_expected", "unknown")  # bias_expected
                            )
                        )
                        articles_inserted += 1
                    except Exception as e:
                        logger.error(f"Error inserting article: {e}")
                        conn.rollback()
                        continue

                conn.commit()
                logger.info(f"Successfully processed {feed['name']}")

            except Exception as e:
                logger.error(f"Error fetching feed {feed['name']}: {e}")
                continue

        cur.close()
        conn.close()
        logger.info(f"Total articles inserted: {articles_inserted}")

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise


with DAG(
    dag_id="rss_news_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["rss", "news"],
    description="Fetch RSS feeds and store articles in PostgreSQL"
) as dag:
    
    ingest_news = PythonOperator(
        task_id="fetch_and_store_rss",
        python_callable=fetch_and_store_rss,
        retries=2,
        retry_delay=60
    )

    trigger_nlp = TriggerDagRunOperator(
        task_id="trigger_nlp_processing",
        trigger_dag_id="nlp_processing_dag",
        wait_for_completion=False
    )
    
    
    # Set dependencies
    ingest_news >> trigger_nlp
