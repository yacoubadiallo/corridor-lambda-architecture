from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when
from pyspark.sql.types import *

# 1. Initialisation
spark = SparkSession.builder \
    .appName("CorridorLambdaArchitecture") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

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

# 2. Lecture du flux Kafka (Source Unique)
print("Connexion au flux Kafka Corridor Mali...")
df_kafka = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "corridor_reviews") \
    .option("startingOffsets", "latest") \
    .load()

# Extraction des données
raw_df = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")).select("data.*")

# 3. Traitement ML (Speed Layer)
# On ajoute une colonne de sentiment basée sur la note
final_results = raw_df.withColumn("sentiment", 
    when(col("note") >= 3, "Satisfait").otherwise("Insatisfait"))

# 4. DOUBLE SORTIE (Le coeur de l'architecture)

print("Activation de la Batch Layer (Archivage JSON)...")
query_archive = raw_df.writeStream \
    .format("json") \
    .option("path", "/opt/spark/data/corridor_history") \
    .option("checkpointLocation", "/opt/spark/data/checkpoints_history") \
    .start()

print("Activation de la Speed Layer (MongoDB)...")
query_mongo = final_results.writeStream \
    .format("mongodb") \
    .option("checkpointLocation", "/opt/spark/data/checkpoints_mongo") \
    .option("spark.mongodb.connection.uri", "mongodb://mongodb:27017") \
    .option("spark.mongodb.database", "corridor_db") \
    .option("spark.mongodb.collection", "results") \
    .outputMode("append") \
    .start()

print("Pipeline Lambda en ligne. En attente de données...")

# Maintenir le script ouvert pour les deux flux
spark.streams.awaitAnyTermination()