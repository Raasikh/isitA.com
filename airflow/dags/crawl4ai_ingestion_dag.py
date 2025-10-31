from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import psycopg2
import logging
import re

logger = logging.getLogger(__name__)

PG_CONN = {
    "host": "postgres_db",
    "database": "app_db",
    "user": "postgres",
    "password": "postgres",
    "port": 5432
}

# URLs to crawl - same tech/cybersecurity sites
CRAWL_URLS = {
    "bbc_tech": {
        "url": "https://www.bbc.com/news/technology",
        "category": "tech_news_mainstream",
        "domain": "News/BBC",
        "bias_expected": "center"
    },
    "reuters_tech": {
        "url": "https://www.reuters.com/technology",
        "category": "tech_news_mainstream",
        "domain": "News/Reuters",
        "bias_expected": "center"
    },
    "bleepingcomputer": {
        "url": "https://www.bleepingcomputer.com/news/",
        "category": "cybersecurity_specialist",
        "domain": "Specialist/BleepingComputer",
        "bias_expected": "center"
    },
    "techcrunch": {
        "url": "https://techcrunch.com/category/security/",
        "category": "tech_industry",
        "domain": "Industry/TechCrunch",
        "bias_expected": "center-left"
    },
    "theverge": {
        "url": "https://www.theverge.com/tech",
        "category": "tech_industry",
        "domain": "Industry/TheVerge",
        "bias_expected": "center"
    },
}

def init_crawled_articles_table():
    """Ensure crawled_articles table exists with content column"""
    try:
        conn = psycopg2.connect(**PG_CONN)
        cur = conn.cursor()
        
        # Add content column if it doesn't exist
        cur.execute("""
            ALTER TABLE news_articles 
            ADD COLUMN IF NOT EXISTS content TEXT;
        """)
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Crawled articles table prepared")
    except Exception as e:
        logger.error(f"Error initializing table: {e}")
        raise

def crawl_articles():
    """Crawl articles using crawl4ai and extract article links"""
    try:
        from crawl4ai import AsyncWebCrawler
        import asyncio
        from bs4 import BeautifulSoup
        
        async def crawl():
            results = []
            async with AsyncWebCrawler() as crawler:
                for source_name, config in CRAWL_URLS.items():
                    try:
                        logger.info(f"Crawling {source_name}: {config['url']}")
                        result = await crawler.arun(config['url'])
                        
                        # Parse HTML to extract article links and titles
                        soup = BeautifulSoup(result.html, 'html.parser')
                        
                        # Find article links (customize selectors per site)
                        articles = soup.find_all('a', {'href': re.compile(r'/(article|news|story)/')}, limit=10)
                        
                        for article in articles:
                            title = article.get_text(strip=True)
                            link = article.get('href')
                            
                            # Make absolute URLs
                            if link and not link.startswith('http'):
                                link = config['url'].split('/news')[0] + link
                            
                            if title and link:
                                results.append({
                                    'source': source_name,
                                    'title': title[:200],  # Limit title length
                                    'link': link,
                                    'url': config['url'],
                                    'html': result.html[:5000] if result.html else None,  # Store partial HTML
                                    'markdown': result.markdown[:2000] if result.markdown else None,
                                    'category': config['category'],
                                    'domain': config['domain'],
                                    'bias_expected': config['bias_expected']
                                })
                    except Exception as e:
                        logger.warning(f"Error crawling {source_name}: {e}")
                        continue
            
            return results
        
        results = asyncio.run(crawl())
        logger.info(f"Crawled {len(results)} articles from {len(CRAWL_URLS)} sites")
        return results
    except Exception as e:
        logger.error(f"Error in crawl_articles: {e}")
        raise

def store_crawled_articles(ti):
    """Store crawled articles in database"""
    results = ti.xcom_pull(task_ids='crawl_articles')
    
    if not results:
        logger.info("No articles to store")
        return
    
    try:
        conn = psycopg2.connect(**PG_CONN)
        cur = conn.cursor()
        
        articles_stored = 0
        for item in results:
            try:
                cur.execute("""
                    INSERT INTO news_articles 
                    (title, link, content, source, category, domain, source_type, bias_expected, published)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (link) DO NOTHING;
                """, (
                    item['title'],
                    item['link'],
                    item['markdown'],  # Store markdown version
                    item['source'],
                    item['category'],
                    item['domain'],
                    'crawled',  # source_type
                    item['bias_expected']
                ))
                articles_stored += 1
            except Exception as e:
                logger.warning(f"Error storing article: {e}")
                conn.rollback()
                continue
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Stored {articles_stored} crawled articles")
    except Exception as e:
        logger.error(f"Error in store_crawled_articles: {e}")
        raise

def display_crawl_stats():
    """Display crawling statistics"""
    try:
        conn = psycopg2.connect(**PG_CONN)
        cur = conn.cursor()
        
        cur.execute("""
            SELECT source, COUNT(*) as count, MAX(published) as latest
            FROM news_articles
            WHERE source_type = 'crawled'
            GROUP BY source
            ORDER BY count DESC;
        """)
        
        stats = cur.fetchall()
        cur.close()
        conn.close()
        
        logger.info("=== Crawled Articles Statistics ===")
        for source, count, latest in stats:
            logger.info(f"{source}: {count} articles (latest: {latest})")
    except Exception as e:
        logger.error(f"Error displaying stats: {e}")

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=60)
}

with DAG(
    dag_id='crawl4ai_ingestion_dag',
    default_args=default_args,
    start_date=datetime(2025, 10, 29),
    schedule='@daily',  # Once per day
    catchup=False,
    description="Crawl tech news sites and store articles"
) as dag:
    
    init_task = PythonOperator(
        task_id='init_crawled_articles_table',
        python_callable=init_crawled_articles_table
    )
    
    crawl_task = PythonOperator(
        task_id='crawl_articles',
        python_callable=crawl_articles,
        execution_timeout=timedelta(minutes=45)
    )
    
    store_task = PythonOperator(
        task_id='store_crawled_articles',
        python_callable=store_crawled_articles
    )
    
    stats_task = PythonOperator(
        task_id='display_crawl_stats',
        python_callable=display_crawl_stats
    )
    
    # Trigger sentiment analysis (same as RSS DAG)
    trigger_sentiment = TriggerDagRunOperator(
        task_id='trigger_sentiment_analysis',
        trigger_dag_id='nlp_processing_dag',
        wait_for_completion=False
    )
    
    init_task >> crawl_task >> store_task >> stats_task >> trigger_sentiment
    
    
