FROM python:3.10-slim
WORKDIR /app
COPY consumer.py .
COPY ../audit_logger.py .
RUN pip install --no-cache-dir kafka-python
CMD ["python", "consumer.py"]
