from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import psycopg2
import logging

logger = logging.getLogger(__name__)

PG_CONN = {
    "host": "postgres_db",
    "database": "app_db",
    "user": "postgres",
    "password": "postgres",
    "port": 5432
}

def init_sentiment_table():
    """Create sentiment table if it doesn't exist"""
    try:
        conn = psycopg2.connect(**PG_CONN)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS article_sentiments (
                id SERIAL PRIMARY KEY,
                article_id INTEGER,
                title TEXT,
                sentiment_label VARCHAR(20),
                sentiment_score FLOAT,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (article_id) REFERENCES news_articles(id)
            )
        """)
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Sentiment table initialized")
    except Exception as e:
        logger.error(f"Error initializing table: {e}")
        raise

def extract_unprocessed_articles():
    """Extract articles that haven't been processed yet"""
    try:
        conn = psycopg2.connect(**PG_CONN)
        cur = conn.cursor()
        
        cur.execute("""
            SELECT id, title, source 
            FROM news_articles 
            WHERE id NOT IN (SELECT DISTINCT article_id FROM article_sentiments)
            ORDER BY published DESC 
            LIMIT 20
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        logger.info(f"Extracted {len(rows)} unprocessed articles")
        return rows
    except Exception as e:
        logger.error(f"Error extracting articles: {e}")
        raise

def run_sentiment_analysis(ti):
    """Analyze sentiment of article titles"""
    articles = ti.xcom_pull(task_ids='extract_unprocessed_articles')
    
    if not articles:
        logger.info("No new articles to process")
        return []
    
    try:
        from transformers import pipeline
        
        sentiment_pipeline = pipeline(
            "sentiment-analysis",
            model="distilbert-base-uncased-finetuned-sst-2-english",
            device=-1
        )
        
        processed = []
        for article_id, title, source in articles:
            try:
                text_to_analyze = title[:512]
                
                result = sentiment_pipeline(text_to_analyze)[0]
                sentiment_label = result['label']
                sentiment_score = result['score']
                
                processed.append({
                    'article_id': article_id,
                    'title': title,
                    'sentiment_label': sentiment_label,
                    'sentiment_score': sentiment_score
                })
                
                logger.info(f"Article {article_id}: {sentiment_label} ({sentiment_score:.2f})")
            except Exception as e:
                logger.warning(f"Error processing article {article_id}: {e}")
                continue
        
        logger.info(f"Processed {len(processed)} articles")
        return processed
    except Exception as e:
        logger.error(f"Error running sentiment analysis: {e}")
        raise

def store_sentiment_results(ti):
    """Store sentiment analysis results in database"""
    processed = ti.xcom_pull(task_ids='run_sentiment_analysis')
    
    if not processed:
        logger.info("No results to store")
        return
    
    try:
        conn = psycopg2.connect(**PG_CONN)
        cur = conn.cursor()
        
        for item in processed:
            cur.execute("""
                INSERT INTO article_sentiments (article_id, title, sentiment_label, sentiment_score)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (
                item['article_id'],
                item['title'],
                item['sentiment_label'],
                item['sentiment_score']
            ))
        
        conn.commit()
        cur.close()
        conn.close()
        
        logger.info(f"Stored {len(processed)} sentiment results")
    except Exception as e:
        logger.error(f"Error storing results: {e}")
        raise

def display_sentiment_stats():
    """Display sentiment statistics"""
    try:
        conn = psycopg2.connect(**PG_CONN)
        cur = conn.cursor()
        
        cur.execute("""
            SELECT sentiment_label, COUNT(*) as count, AVG(sentiment_score) as avg_score
            FROM article_sentiments
            GROUP BY sentiment_label
        """)
        stats = cur.fetchall()
        cur.close()
        conn.close()
        
        logger.info("=== Sentiment Statistics ===")
        for label, count, avg_score in stats:
            logger.info(f"{label}: {count} articles (avg score: {avg_score:.3f})")
    except Exception as e:
        logger.error(f"Error displaying stats: {e}")

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30)
}

with DAG(
    dag_id='nlp_processing_dag',
    default_args=default_args,
    start_date=datetime(2024, 10, 25),
    schedule=None,
    catchup=False,
    description="Perform sentiment analysis on news articles"
) as dag:
    
    init_table = PythonOperator(
        task_id='init_sentiment_table',
        python_callable=init_sentiment_table
    )
    
    extract_task = PythonOperator(
        task_id='extract_unprocessed_articles',
        python_callable=extract_unprocessed_articles
    )
    
    sentiment_task = PythonOperator(
        task_id='run_sentiment_analysis',
        python_callable=run_sentiment_analysis
    )
    
    store_task = PythonOperator(
        task_id='store_sentiment_results',
        python_callable=store_sentiment_results
    )
    
    stats_task = PythonOperator(
        task_id='display_sentiment_stats',
        python_callable=display_sentiment_stats
    )
    
    trigger_clustering = TriggerDagRunOperator(
        task_id='trigger_topic_clustering',
        trigger_dag_id='topic_clustering_dag',
        wait_for_completion=False
    )
