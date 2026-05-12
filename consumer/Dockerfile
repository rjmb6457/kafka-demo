FROM python:3.11-slim
WORKDIR /app
COPY audit_logger.py .
COPY consumer/consumer.py .
RUN pip install --no-cache-dir kafka-python
CMD ["python", "consumer.py"]
