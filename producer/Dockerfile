FROM python:3.11-slim
WORKDIR /app
COPY audit_logger.py .
COPY producer/producer.py .
RUN pip install --no-cache-dir kafka-python
CMD ["python", "producer.py"]
