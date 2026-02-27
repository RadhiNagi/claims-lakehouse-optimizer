# ============================================================
# Dockerfile: API + PySpark + Delta Lake
# ============================================================
# STORY: This single container has EVERYTHING:
#   - Python 3.11 (stable, industry standard)
#   - FastAPI (web API framework)
#   - PySpark (big data processing)
#   - Delta Lake (smart table format)
#   - Java 17 (Spark needs Java to run)
#
# WHY Java? Spark is written in Scala which runs on Java.
#   PySpark is a Python wrapper around Spark.
#   So: Your Python code → PySpark → Spark (Java) → Data
# ============================================================

FROM python:3.11-slim

WORKDIR /app

# Install Java (required by Spark) and basic tools
# STORY: Like installing the oven (Java) before the chef (Spark) can cook
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Set Java home (Spark needs to know where Java is)
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project code
COPY . .

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]