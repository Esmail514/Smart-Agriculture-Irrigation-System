FROM python:3.12-slim
WORKDIR /app
ENV PYTHONPATH=/app
RUN pip install --no-cache-dir kafka-python-ng psycopg2-binary==2.9.10
COPY src/common /app/src/common
COPY src/ingestion /app/src/ingestion
USER 65532:65532
CMD ["python", "-m", "src.ingestion.sensor_simulator.simulator"]
