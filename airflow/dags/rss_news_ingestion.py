from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import feedparser
import psycopg2
from email.utils import parsedate_to_datetime
import logging

logger = logging.getLogger(__name__)

RSS_FEEDS_CONFIG = [
    {
        "name": "cisa",
        "url": "https://www.cisa.gov/news.xml",
        "category": "cybersecurity_government",
        "primary_domain": "Government/CISA",
    },
    {
        "name": "nist",
        "url": "https://www.nist.gov/news-events/resilience/rss.xml",
        "category": "cybersecurity_standards",
        "primary_domain": "Standards/NIST",
    }
]

PG_CONN = {
    "host": "postgres_db",
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
                            INSERT INTO news_articles (source, title, link, published, category, domain)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (link) DO NOTHING;
                            """,
                            (
                                feed["name"],
                                title,
                                link,
                                published,
                                feed["category"],
                                feed["primary_domain"]
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
    dag_id='rss_news_ingestion',
    start_date=datetime(2024, 1, 1),
    schedule='@hourly',
    catchup=False,
    tags=['rss', 'news'],
    description="Fetch RSS feeds and store articles in PostgreSQL"
) as dag:
    
    fetch_news = PythonOperator(
        task_id='fetch_and_store_rss',
        python_callable=fetch_and_store_rss,
        retries=2,
        retry_delay=60
    )
    
    trigger_nlp = TriggerDagRunOperator(
        task_id='trigger_nlp_processing',
        trigger_dag_id='nlp_processing_dag',
        wait_for_completion=False
    )
    
    
    fetch_news >> trigger_nlp
