Markdown
# ⛽ Corridor Service Analytics: Real-Time Multi-Service Pipeline
[![Spark](https://img.shields.io/badge/Apache_Spark-3.5.0-orange?style=flat&logo=apachespark)](https://spark.apache.org/)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-7.5.0-black?style=flat&logo=apachekafka)](https://kafka.apache.org/)
[![MongoDB](https://img.shields.io/badge/MongoDB-6.0-green?style=flat&logo=mongodb)](https://www.mongodb.com/)
[![Docker](https://img.shields.io/badge/Docker-Orchestrated-blue?style=flat&logo=docker)](https://www.docker.com/)

## 📝 Présentation
Ce projet déploie une **Architecture Lambda** complète pour le monitoring de l'expérience client au sein des stations-service au Mali. Le système analyse la performance de l'ensemble des pôles d'activité (Carburant, Boutique, Lavage, Gaz).

## 🖼️ Schéma de l'Architecture (Data Pipeline)

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
🚀 Déploiement
1. Lancement de l'infrastructure
Bash
docker-compose up -d --build
2. Démarrage du Pipeline de Traitement
Bash
docker exec -it spark-master-1 spark-submit \
  --master spark://spark-master-1:7077,spark-master-2:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
  /opt/spark/apps/sentiment_analysis.py
3. Compactage vers le Data Lake (Batch Layer)
Pour optimiser le stockage et préparer les données pour le Machine Learning :

Bash
docker exec -it spark-master-1 spark-submit /opt/spark/apps/compact_to_parquet.py
📊 Points d'accès Monitoring
Dashboard Streamlit : http://localhost:8501

Spark Master UI : http://localhost:8080

Mongo Express : http://localhost:8082

💡 Défis Techniques Relevés
Haute Disponibilité : Cluster Spark avec 2 Masters (via Zookeeper) et 5 Workers.

Schéma Évolutif : Utilisation du format JSON pour capturer la diversité des avis sans contrainte de schéma rigide.

Optimisation Storage : Implémentation du format Parquet (format colonne) réduisant l'espace disque et accélérant les requêtes.