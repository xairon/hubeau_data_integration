FROM python:3.11-slim

WORKDIR /app

# System deps for psycopg2 and scipy
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Install Python deps (CPU-only torch to keep image small)
RUN pip install --no-cache-dir \
    "psycopg2-binary>=2.9.0" \
    "pandas>=2.2.0" \
    "numpy>=1.26.0" \
    "pyarrow>=15.0.0" \
    "scikit-learn>=1.3.0" \
    "umap-learn>=0.5.0" \
    "tqdm>=4.65.0" \
    "plotly>=5.18.0" \
    "streamlit>=1.30.0" \
    "python-dotenv>=1.0.0" \
    "scipy>=1.11.0" && \
    pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu

# Copy source (overridden by volume mounts in dev)
COPY benchmark/ .

ENV PYTHONPATH=/app/src

EXPOSE 8501

CMD ["streamlit", "run", "app/app.py", "--server.port=8501", "--server.headless=true", "--server.address=0.0.0.0"]
