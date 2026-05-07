FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=35100 \
    DATABASE_PATH=/app/data/pc28touzhu.db

COPY pyproject.toml README.md ./
COPY src ./src
COPY pc28 fake_executor.py platform_alert_notifier.py platform_auto_trigger.py platform_source_sync.py telegram_daily_reporter.py telegram_executor.py telegram_profit_bot.py seed_demo.py ./

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -e ".[telegram]" \
    && mkdir -p /app/data

EXPOSE 35100

CMD ["python", "-m", "pc28touzhu.main"]
