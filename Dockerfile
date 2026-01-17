FROM python:3.12-slim as builder

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

FROM python:3.12-slim

WORKDIR /app

COPY --from=builder /root/.local /root/.local
ENV PATH=/root/.local/bin:$PATH

COPY src/ ./src/
COPY dashboard/ ./dashboard/

RUN mkdir -p /app/data

ENV PYTHONUNBUFFERED=1
ENV DATABASE_PATH=/app/data/jellytrack.db

EXPOSE 8085

CMD ["python", "-m", "src.main"]
