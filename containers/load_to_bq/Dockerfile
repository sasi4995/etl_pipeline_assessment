FROM python:3.9-slim

WORKDIR /app

COPY scripts/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scripts/ .

CMD ["python", "load_to_bq.py"]
