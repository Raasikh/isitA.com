FROM apache/airflow:3.0.1

WORKDIR /opt/airflow

# Copy Python requirements
COPY requirements-airflow.txt /opt/airflow/

# Switch to root to install system dependencies
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user for Python package installation
USER airflow
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /opt/airflow/requirements-airflow.txt

# Switch to root to install Playwright browsers (needed for crawl4ai or scraping DAGs)
USER root
RUN playwright install --with-deps

# Switch back to airflow user
USER airflow

# Copy the rest of the project
COPY . /opt/airflow

# Default Airflow entrypoint
ENTRYPOINT ["/entrypoint"]
CMD ["bash"]