from airflow import DAG
from airflow.operators.python import PythonOperator
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

def init_bias_table():
    """Create bias classification table"""
    try:
        conn = psycopg2.connect(**PG_CONN)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS article_bias (
                id SERIAL PRIMARY KEY,
                article_id INTEGER UNIQUE,
                title TEXT,
                bias_label VARCHAR(20),  -- 'left', 'center', 'right'
                bias_score FLOAT,         -- -1 to 1 scale
                confidence FLOAT,         -- 0-1 confidence
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (article_id) REFERENCES news_articles(id)
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS cluster_bias_stats (
                cluster_id INTEGER PRIMARY KEY,
                avg_bias_score FLOAT,
                left_count INTEGER,
                center_count INTEGER,
                right_count INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Bias tables initialized")
    except Exception as e:
        logger.error(f"Error initializing tables: {e}")
        raise

def extract_unbiased_articles():
    """Extract articles without bias classification yet"""
    try:
        conn = psycopg2.connect(**PG_CONN)
        cur = conn.cursor()
        
        cur.execute("""
            SELECT n.id, n.title, n.source
            FROM news_articles n
            WHERE n.id NOT IN (SELECT DISTINCT article_id FROM article_bias)
            ORDER BY n.created_at DESC
            LIMIT 30
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        logger.info(f"Extracted {len(rows)} unbiased articles")
        return rows
    except Exception as e:
        logger.error(f"Error extracting articles: {e}")
        raise

def run_bias_classification(ti):
    """Classify articles as left/center/right using zero-shot classification"""
    try:
        from transformers import pipeline
        
        articles = ti.xcom_pull(task_ids='extract_unbiased_articles')
        
        if not articles:
            logger.info("No articles to classify")
            return []
        
        # Use zero-shot classifier - works with any labels
        logger.info("Loading zero-shot classification model...")
        classifier = pipeline(
            "zero-shot-classification",
            model="facebook/bart-large-mnli",
            device=-1  # CPU
        )
        
        candidate_labels = ["left-leaning", "center", "right-leaning"]
        results = []
        
        for article_id, title, source in articles:
            try:
                # Use title for classification (shorter, faster)
                text = title[:512]
                
                prediction = classifier(
                    text,
                    candidate_labels,
                    multi_class=False
                )
                
                # Map to bias label and score
                bias_label = prediction['labels'][0]
                confidence = prediction['scores'][0]
                
                # Convert to numerical score: -1 (left) to +1 (right)
                if "left" in bias_label.lower():
                    bias_score = -1.0
                elif "center" in bias_label.lower():
                    bias_score = 0.0
                else:  # right
                    bias_score = 1.0
                
                results.append({
                    'article_id': article_id,
                    'title': title,
                    'source': source,
                    'bias_label': bias_label,
                    'bias_score': bias_score,
                    'confidence': confidence
                })
                
                logger.info(f"Article {article_id}: {bias_label} (confidence: {confidence:.2f})")
            except Exception as e:
                logger.warning(f"Error classifying article {article_id}: {e}")
                continue
        
        logger.info(f"Classified {len(results)} articles")
        return results
    except Exception as e:
        logger.error(f"Error in bias classification: {e}")
        raise

def store_bias_results(ti):
    """Store bias classification results"""
    results = ti.xcom_pull(task_ids='run_bias_classification')
    
    if not results:
        logger.info("No results to store")
        return
    
    try:
        conn = psycopg2.connect(**PG_CONN)
        cur = conn.cursor()
        
        for item in results:
            cur.execute("""
                INSERT INTO article_bias (article_id, title, bias_label, bias_score, confidence)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (article_id) DO NOTHING
            """, (
                item['article_id'],
                item['title'],
                item['bias_label'],
                item['bias_score'],
                item['confidence']
            ))
        
        conn.commit()
        cur.close()
        conn.close()
        
        logger.info(f"Stored {len(results)} bias classifications")
    except Exception as e:
        logger.error(f"Error storing bias results: {e}")
        raise

def aggregate_cluster_bias():
    """Calculate average bias per cluster"""
    try:
        conn = psycopg2.connect(**PG_CONN)
        cur = conn.cursor()
        
        # Get bias distribution per cluster
        cur.execute("""
            SELECT 
                ac.cluster_id,
                AVG(ab.bias_score) as avg_bias,
                COUNT(CASE WHEN ab.bias_label LIKE '%left%' THEN 1 END) as left_count,
                COUNT(CASE WHEN ab.bias_label LIKE '%center%' THEN 1 END) as center_count,
                COUNT(CASE WHEN ab.bias_label LIKE '%right%' THEN 1 END) as right_count
            FROM article_clusters ac
            LEFT JOIN article_bias ab ON ac.article_id = ab.article_id
            WHERE ab.bias_label IS NOT NULL
            GROUP BY ac.cluster_id
        """)
        
        results = cur.fetchall()
        
        for cluster_id, avg_bias, left_count, center_count, right_count in results:
            cur.execute("""
                INSERT INTO cluster_bias_stats (cluster_id, avg_bias_score, left_count, center_count, right_count)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (cluster_id) DO UPDATE SET
                    avg_bias_score = EXCLUDED.avg_bias_score,
                    left_count = EXCLUDED.left_count,
                    center_count = EXCLUDED.center_count,
                    right_count = EXCLUDED.right_count
            """, (cluster_id, avg_bias, left_count or 0, center_count or 0, right_count or 0))
        
        conn.commit()
        cur.close()
        conn.close()
        
        logger.info(f"Aggregated bias stats for {len(results)} clusters")
        
        # Log stats
        logger.info("=== Cluster Bias Distribution ===")
        for cluster_id, avg_bias, left_count, center_count, right_count in results:
            logger.info(f"Cluster {cluster_id}: Avg Bias {avg_bias:.2f} | Left: {left_count}, Center: {center_count}, Right: {right_count}")
            
    except Exception as e:
        logger.error(f"Error aggregating cluster bias: {e}")
        raise

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=45)
}

with DAG(
    dag_id='bias_detection_dag',
    default_args=default_args,
    start_date=datetime(2025, 10, 29),
    schedule=None,
    catchup=False,
    description="Classify articles by political bias"
) as dag:
    
    init_table = PythonOperator(
        task_id='init_bias_table',
        python_callable=init_bias_table
    )
    
    extract_task = PythonOperator(
        task_id='extract_unbiased_articles',
        python_callable=extract_unbiased_articles
    )
    
    classify_task = PythonOperator(
        task_id='run_bias_classification',
        python_callable=run_bias_classification
    )
    
    store_task = PythonOperator(
        task_id='store_bias_results',
        python_callable=store_bias_results
    )
    
    aggregate_task = PythonOperator(
        task_id='aggregate_cluster_bias',
        python_callable=aggregate_cluster_bias
    )
    
    init_table >> extract_task >> classify_task >> store_task >> aggregate_task
