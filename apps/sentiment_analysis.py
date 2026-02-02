from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when
from pyspark.sql.types import *

# 1. Initialisation avec optimisation Spark 3.5
spark = SparkSession.builder \
    .appName("CorridorLambdaArchitecture") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

# Niveau de log réduit pour éviter de polluer ton terminal
spark.sparkContext.setLogLevel("ERROR") 

# Schéma des données
schema = StructType([
    StructField("id_client", StringType()),
    StructField("station", StringType()),
    StructField("service", StringType()),
    StructField("avis", StringType()),
    StructField("note", IntegerType()),
    StructField("moyen_paiement", StringType()),
    StructField("timestamp", DoubleType())
])

# 2. Lecture du flux Kafka
print(">>> Connexion au flux Kafka Corridor Mali...")
df_kafka = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "corridor_reviews") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Extraction et parsing JSON
raw_df = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")).select("data.*")

# 3. Speed Layer Logic (Transformation)
final_results = raw_df.withColumn("sentiment", 
    when(col("note") >= 3, "Satisfait").otherwise("Insatisfait"))

# 4. DOUBLE SORTIE (Architecture Lambda)

# --- Batch Layer (Data Lake - Archivage) ---
print(">>> Activation de la Batch Layer (Archivage JSON)...")
query_archive = raw_df.writeStream \
    .format("json") \
    .option("path", "/opt/spark/data/corridor_history") \
    .option("checkpointLocation", "/opt/spark/data/checkpoints_history_v2") \
    .start()

# --- Speed Layer (Dashboard - MongoDB) ---
print(">>> Activation de la Speed Layer (MongoDB)...")
query_mongo = final_results.writeStream \
    .format("mongodb") \
    .option("checkpointLocation", "/opt/spark/data/checkpoints_mongo_v2") \
    .option("connection.uri", "mongodb://mongodb:27017") \
    .option("database", "corridor_db") \
    .option("collection", "results") \
    .outputMode("append") \
    .start()

print(">>> Pipeline Lambda en ligne. En attente de données...")

# Attendre la fin des deux flux
spark.streams.awaitAnyTermination()