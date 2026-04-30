import os
import shutil
import sys

# --- 0. FORCE JAVA 17 (Ton chemin spécifique Adoptium) ---
os.environ["JAVA_HOME"] = r"C:\Users\TOUTENUN\AppData\Local\Programs\Eclipse Adoptium\jdk-17.0.17.10-hotspot"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "\\bin;" + os.environ["PATH"]

# --- 1. CONFIGURATION DES PACKAGES ---
# Utilisation de Scala 2.12 et Spark 3.5.0 (stable)
packages = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "org.postgresql:postgresql:42.7.1"
]
os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages {",".join(packages)} pyspark-shell'

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import *

CHECKPOINT_DIR = "checkpoints_spark/processor_final"

# Nettoyage automatique du checkpoint pour éviter les erreurs de métadonnées
if os.path.exists("checkpoints_spark"):
    shutil.rmtree("checkpoints_spark", ignore_errors=True)

# --- 2. SESSION SPARK (Correctifs Hadoop, Master:9000 et BlockManager) ---
spark = SparkSession.builder \
    .appName("Pipeline_Kafka_To_Postgres_Master") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
    .getOrCreate()

# On cache les logs inutiles
spark.sparkContext.setLogLevel("ERROR")

# Vérification console de la version Java utilisée
java_ver = spark._jvm.java.lang.System.getProperty('java.version')
print(f"\n🚀 MOTEUR SPARK {spark.version} PRÊT SUR JAVA {java_ver}")
print("📦 RÉPLICATION ET FRAGMENTATION ACTIVES\n")

# --- 3. SCHÉMA DES DONNÉES ---
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("montant", DoubleType(), True),
    StructField("pays", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# --- 4. LECTURE DEPUIS KAFKA ---
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions_brutes") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# --- 5. TRANSFORMATION ---
processed_df = raw_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", to_timestamp(col("timestamp")))

# --- 6. FONCTION D'ÉCRITURE VERS POSTGRESQL (MASTER) ---
def write_to_postgres(batch_df, batch_id):
    if not batch_df.isEmpty():
        print(f"📦 Batch {batch_id} : {batch_df.count()} lignes reçues.")
        try:
            # On écrit dans la table parente 'transactions_partitioned'
            # Postgres s'occupe de la fragmentation (partitions) automatiquement
            batch_df.write \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://localhost:5432/fraud_db") \
                .option("dbtable", "transactions_partitioned") \
                .option("user", "admin") \
                .option("password", "password") \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            print("✅ Données stockées sur le Master (Aiguillage vers fragments OK)")
        except Exception as e:
            print(f"❌ Erreur JDBC sur le Master : {e}")

# --- 7. LANCEMENT DES FLUX ---
print("📡 Écoute du topic 'transactions_brutes' en cours...\n")

# Sortie 1 : Console pour voir les données passer
query_console = processed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(processingTime='5 seconds') \
    .start()

# Sortie 2 : PostgreSQL Master (Port 5432)
query_postgres = processed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .trigger(processingTime='5 seconds') \
    .start()

spark.streams.awaitAnyTermination()
