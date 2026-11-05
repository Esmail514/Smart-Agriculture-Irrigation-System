FROM python:3.12-slim
WORKDIR /app
COPY requirements-api.txt requirements-control.txt /app/
RUN pip install --no-cache-dir -r /app/requirements-control.txt
COPY src/control_agent /app/src/control_agent
EXPOSE 8010
CMD ["uvicorn", "src.control_agent.main:app", "--host", "0.0.0.0", "--port", "8010"]
