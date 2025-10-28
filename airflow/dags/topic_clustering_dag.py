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

def init_clustering_table():
    """Create clustering table if it doesn't exist"""
    try:
        conn = psycopg2.connect(**PG_CONN)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS article_clusters (
                id SERIAL PRIMARY KEY,
                article_id INTEGER,
                title TEXT,
                sentiment_label VARCHAR(20),
                cluster_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (article_id) REFERENCES news_articles(id)
            )
        """)
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Clustering table initialized")
    except Exception as e:
        logger.error(f"Error initializing table: {e}")
        raise

def extract_articles_for_clustering():
    """Extract articles with sentiment analysis but not yet clustered"""
    try:
        conn = psycopg2.connect(**PG_CONN)
        cur = conn.cursor()
        
        cur.execute("""
            SELECT s.article_id, s.title, s.sentiment_label
            FROM article_sentiments s
            WHERE s.article_id NOT IN (SELECT DISTINCT article_id FROM article_clusters)
            LIMIT 50
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        logger.info(f"Extracted {len(rows)} articles for clustering")
        return rows
    except Exception as e:
        logger.error(f"Error extracting articles: {e}")
        raise

def cluster_articles(ti):
    """Perform KMeans clustering on article embeddings"""
    try:
        from sklearn.cluster import KMeans
        from sentence_transformers import SentenceTransformer
        import numpy as np
        
        articles = ti.xcom_pull(task_ids='extract_articles_for_clustering')
        
        if not articles or len(articles) < 5:
            logger.info("Not enough articles to cluster")
            return []
        
        logger.info(f"Loading SentenceTransformer model...")
        model = SentenceTransformer('all-MiniLM-L6-v2')
        
        # Extract titles for embedding
        texts = [title for _, title, _ in articles]
        
        logger.info(f"Encoding {len(texts)} texts to embeddings...")
        embeddings = model.encode(texts, convert_to_numpy=True)
        
        # Determine optimal clusters (min 3, max 10)
        n_clusters = min(max(3, len(articles) // 5), 10)
        logger.info(f"Clustering into {n_clusters} clusters")
        
        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        clusters = kmeans.fit_predict(embeddings)
        
        # Return article_id with cluster assignment
        clustered = [
            {
                'article_id': articles[i][0],
                'title': articles[i][1],
                'sentiment': articles[i][2],
                'cluster_id': int(clusters[i])
            }
            for i in range(len(articles))
        ]
        
        logger.info(f"Clustering complete. Results: {len(clustered)} articles")
        return clustered
        
    except Exception as e:
        logger.error(f"Error clustering articles: {e}")
        raise

def store_clusters(ti):
    """Store cluster assignments in database"""
    clustered = ti.xcom_pull(task_ids='cluster_articles')
    
    if not clustered:
        logger.info("No clusters to store")
        return
    
    try:
        conn = psycopg2.connect(**PG_CONN)
        cur = conn.cursor()
        
        for item in clustered:
            cur.execute("""
                INSERT INTO article_clusters (article_id, title, sentiment_label, cluster_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (
                item['article_id'],
                item['title'],
                item['sentiment'],
                item['cluster_id']
            ))
        
        conn.commit()
        cur.close()
        conn.close()
        
        logger.info(f"Stored {len(clustered)} cluster assignments")
    except Exception as e:
        logger.error(f"Error storing clusters: {e}")
        raise

def display_cluster_stats(ti):
    """Display clustering statistics"""
    try:
        conn = psycopg2.connect(**PG_CONN)
        cur = conn.cursor()
        
        cur.execute("""
            SELECT cluster_id, COUNT(*) as count, 
                   STRING_AGG(DISTINCT sentiment_label, ', ') as sentiments
            FROM article_clusters
            GROUP BY cluster_id
            ORDER BY cluster_id
        """)
        stats = cur.fetchall()
        cur.close()
        conn.close()
        
        logger.info("=== Cluster Statistics ===")
        for cluster_id, count, sentiments in stats:
            logger.info(f"Cluster {cluster_id}: {count} articles | Sentiments: {sentiments}")
    except Exception as e:
        logger.error(f"Error displaying stats: {e}")

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30)
}

with DAG(
    dag_id='topic_clustering_dag',
    default_args=default_args,
    start_date=datetime(2025, 10, 28),
    schedule=None,
    catchup=False,
    description="Cluster articles by topic using KMeans on embeddings"
) as dag:
    
    init_table = PythonOperator(
        task_id='init_clustering_table',
        python_callable=init_clustering_table
    )
    
    extract_task = PythonOperator(
        task_id='extract_articles_for_clustering',
        python_callable=extract_articles_for_clustering
    )
    
    cluster_task = PythonOperator(
        task_id='cluster_articles',
        python_callable=cluster_articles
    )
    
    store_task = PythonOperator(
        task_id='store_clusters',
        python_callable=store_clusters
    )
    
    stats_task = PythonOperator(
        task_id='display_cluster_stats',
        python_callable=display_cluster_stats
    )
    
    init_table >> extract_task >> cluster_task >> store_task >> stats_task
