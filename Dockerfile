# FROM python:3.11-slim

# WORKDIR /app

# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

# COPY ./app ./app

# EXPOSE 8000

# # CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
# # CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
# CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port $PORT"]
# # CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]




FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./app ./app

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "debug"]