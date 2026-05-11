FROM python:3.10-slim
WORKDIR /app
COPY producer.py .
COPY ../audit_logger.py .
RUN pip install --no-cache-dir kafka-python
CMD ["python", "producer.py"]
