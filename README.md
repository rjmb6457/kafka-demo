# Kafka Demo Project

## Overview
This project demonstrates a messaging infrastructure using **Apache Kafka** with Python producer/consumer applications.  
It also showcases **automated deployment via CI/CD** using GitHub Actions, Docker, and Kubernetes.

The demo simulates a real-world scenario where JSON batch files arrive in an `extract` directory, are ingested into Kafka topics, processed by consumers with deduplication and fault tolerance, and then archived.

---

## Features
- **Messaging Infrastructure**
  - Kafka cluster setup via Docker Compose
  - Python producer/consumer applications
  - Deduplication for exactly-once delivery
  - Retry logic and Dead Letter Queue (DLQ) handling
  - File-based ingestion workflow (`extract` → Kafka → `archive`)

- **Automated Deployment**
  - GitHub Actions CI/CD pipeline
  - Docker containerization of producer/consumer apps
  - Kubernetes deployment for scalability
  - Automatic redeployment on every commit to `main`

---

## Project Structure
dbs_demo/
├── extract/          # Incoming JSON batch files
├── archive/          # Processed files renamed with _done
├── producer.py       # Python producer app
├── consumer.py       # Python consumer app
├── generate_files.sh # Script to simulate incoming files
├── requirements.txt  # Python dependencies
├── Dockerfile        # Containerization setup
├── k8s-deployment.yaml # Kubernetes deployment config
└── .github/workflows/deploy.yml # GitHub Actions pipeline


---

## 🚀 How to Run Locally
1. Start Kafka cluster:
   ```bash
   docker-compose up -d
2. Generate sample files:
   ```bash
   ./generate_files.sh
3. Run producer:
   ```bash
   python producer.py
4. Run consumer:
  ```bash
   python consumer.py
