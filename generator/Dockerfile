# generator/Dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY data_generator.py .
RUN pip install --no-cache-dir kafka-python
CMD ["python", "-u", "data_generator.py"]