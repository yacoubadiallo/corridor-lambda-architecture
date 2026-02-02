# ⛽ Corridor Service Analytics: Real-Time Multi-Service Pipeline
[![Spark](https://img.shields.io/badge/Apache_Spark-3.5.0-orange?style=flat&logo=apachespark)](https://spark.apache.org/)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-7.5.0-black?style=flat&logo=apachekafka)](https://kafka.apache.org/)
[![MongoDB](https://img.shields.io/badge/MongoDB-6.0-green?style=flat&logo=mongodb)](https://www.mongodb.com/)
[![Docker](https://img.shields.io/badge/Docker-Orchestrated-blue?style=flat&logo=docker)](https://www.docker.com/)

## 📝 Présentation
Ce projet déploie une **Architecture Lambda** complète pour le monitoring de l'expérience client au sein des stations-service au Mali. Le système analyse la performance de l'ensemble des pôles d'activité (Carburant, Boutique, Lavage, Gaz).

## 🏗️ Flux de Données (Data Pipeline)

```mermaid
graph LR
    subgraph "Ingestion"
        A[Python Producer] -->|Avis Clients| B(Apache Kafka)
    end

    subgraph "Processing (Spark Cluster)"
        B --> C{Spark Structured Streaming}
        C -->|Speed Layer| D[MongoDB]
        C -->|Batch Layer - Raw| E[(Data Lake JSON)]
        E -->|Compaction Job| F[(Data Lake Parquet)]
    end

    subgraph "Visualization"
        D --> G[Streamlit Dashboard]
        F --> H[Historique / ML Ready]
    end