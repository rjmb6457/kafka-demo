FROM python:3.11-slim
WORKDIR /app
COPY dlq_reprocessor.py .
COPY ../audit_logger.py .
RUN pip install --no-cache-dir kafka-python
CMD ["python", "dlq_reprocessor.py"]