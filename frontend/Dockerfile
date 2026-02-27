# ============================================
# Dockerfile for Streamlit Dashboard (Frontend)
# ============================================

FROM python:3.11-slim

WORKDIR /app

# Install Streamlit and HTTP library
RUN pip install --no-cache-dir \
    streamlit==1.39.0 \
    requests==2.32.0 \
    plotly==5.24.0 \
    pandas==2.2.3

# Copy frontend code
COPY . .

EXPOSE 8501

# Streamlit runs differently than FastAPI
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]