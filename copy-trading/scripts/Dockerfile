FROM python:3.12-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir asyncpg websockets httpx

CMD ["python", "agent_trader.py"]
