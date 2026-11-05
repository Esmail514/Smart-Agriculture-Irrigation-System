FROM python:3.12-slim
WORKDIR /app
COPY requirements-api.txt /app/requirements-api.txt
RUN pip install --no-cache-dir -r /app/requirements-api.txt
COPY src/api /app/src/api
COPY scripts/migrate_api_schema.py /app/scripts/migrate_api_schema.py
COPY sql/postgres/init/02-api-schema.sql /app/sql/postgres/init/02-api-schema.sql
USER 65532:65532
EXPOSE 8000
CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
