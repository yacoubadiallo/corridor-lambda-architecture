from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

# Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("Batch-Compaction-JSON-to-Parquet") \
    .getOrCreate()

print("--- Début du compactage : Bronze (JSON) vers Silver (Parquet) ---")

# 1. Lecture de tous les fichiers JSON du dossier history
# Spark est intelligent : il va fusionner tous les petits fichiers en un seul DataFrame
df_bronze = spark.read.json("/opt/spark/data/corridor_history/*.json")

# 2. Petite transformation (Optionnel mais recommandé) : s'assurer que le timestamp est au bon format
if "timestamp" in df_bronze.columns:
    df_silver = df_bronze.withColumn("timestamp", to_timestamp(col("timestamp")))
else:
    df_silver = df_bronze

# 3. Écriture au format Parquet
# 'overwrite' permet de rafraîchir le Lake à chaque lancement du script
df_silver.write \
    .mode("overwrite") \
    .parquet("/opt/spark/data/corridor_lake_silver")

print(f"Compactage terminé ! {df_silver.count()} messages archivés dans /opt/spark/data/corridor_lake_silver")

spark.stop()