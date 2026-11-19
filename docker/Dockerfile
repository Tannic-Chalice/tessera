# One image for every TESSERA Python service (producer, fast plane, gate, slow plane, sink, dashboard).
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY tessera ./tessera
COPY db ./db
COPY tests ./tests

CMD ["python", "-m", "tessera.services.topic_init"]
