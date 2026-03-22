FROM python:3.9-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

EXPOSE 8000

CMD ["sh", "-c", "(crontab -l 2>/dev/null || true; echo '0 0 * * * python -m app.services.parser_service.ParserService /app/data/app.db daily_update') | crontab - && service cron start && python -m uvicorn app.main:app --host 0.0.0.0 --port 8000"]

