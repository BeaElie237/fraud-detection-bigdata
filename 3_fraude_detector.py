import os
import shutil
import sys
import pandas as pd
import xgboost as xgb

# --- 0. CONFIGURATION ENVIRONNEMENT (Force Python et Java) ---
# Chemin Java
os.environ["JAVA_HOME"] = r"C:\Users\TOUTENUN\AppData\Local\Programs\Eclipse Adoptium\jdk-17.0.17.10-hotspot"
# Chemin Python (Force l'exécutable actuel pour Spark)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

os.environ["PATH"] = os.environ["JAVA_HOME"] + "\\bin;" + os.environ["PATH"]

# Configuration des packages
packages = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "org.postgresql:postgresql:42.7.1"
]
os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages {",".join(packages)} pyspark-shell'

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import *

# --- 1. MAPPING DES 30 PAYS ---
mapping_pays = {
    'France': 1, 'USA': 2, 'Canada': 3, 'Maroc': 4, 'Allemagne': 5, 'Japon': 6, 
    'Royaume-Uni': 7, 'Italie': 8, 'Espagne': 9, 'Bresil': 10, 'Australie': 11, 
    'Chine': 12, 'Inde': 13, 'Russie': 14, 'Sénégal': 15, 'Cameroun': 16, 
    'Côte d\'Ivoire': 17, 'Belgique': 18, 'Suisse': 19, 'Mexique': 20, 
    'Argentine': 21, 'Egypte': 22, 'Nigeria': 23, 'Afrique du Sud': 24, 
    'Portugal': 25, 'Grèce': 26, 'Turquie': 27, 'Corée du Sud': 28, 
    'Thaïlande': 29, 'Vietnam': 30
}

# --- 2. CHARGEMENT DU MODÈLE XGBOOST ---
clf = xgb.XGBClassifier()
if os.path.exists("fraude_model.json"):
    clf.load_model("fraude_model.json")
else:
    print("❌ Erreur : Fichier 'fraude_model.json' introuvable !")
    sys.exit(1)

# --- 3. UDF DE PRÉDICTION ---
def predict_fraud_logic(montant, pays_nom, ts):
    if ts is None or montant is None: return 0
    try:
        heure = ts.hour
        code_p = mapping_pays.get(pays_nom, 99)
        # Format attendu par le modèle
        input_df = pd.DataFrame([[montant, heure, code_p]], 
                               columns=['montant', 'heure', 'code_pays'])
        return int(clf.predict(input_df)[0])
    except:
        return 0

predict_udf = udf(predict_fraud_logic, IntegerType())

# --- 4. SESSION SPARK (Correctifs Timeout et Réseau) ---
spark = SparkSession.builder \
    .appName("ML_RealTime_Predictor") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.network.timeout", "1000s") \
    .config("spark.executor.heartbeatInterval", "100s") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# --- 5. PIPELINE STREAMING ---
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("montant", DoubleType(), True),
    StructField("pays", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions_brutes") \
    .option("startingOffsets", "latest") \
    .load()

predictions_df = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")).select("data.*") \
    .filter(col("id").isNotNull()) \
    .withColumn("is_fraud", predict_udf(col("montant"), col("pays"), col("timestamp")))

# --- 6. ÉCRITURE VERS POSTGRES ---
def process_batch(batch_df, batch_id):
    if not batch_df.isEmpty():
        print(f"🧠 Batch {batch_id} en cours d'analyse...")
        try:
            batch_df.write \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://localhost:5432/fraud_db") \
                .option("dbtable", "predictions_ia") \
                .option("user", "admin") \
                .option("password", "password") \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            
            f_count = batch_df.filter(col("is_fraud") == 1).count()
            if f_count > 0:
                print(f"🚨 ALERTES : {f_count} fraudes détectées et enregistrées !")
        except Exception as e:
            print(f"❌ Erreur JDBC : {e}")

# --- 7. LANCEMENT ---
print("\n🔍 DÉTECTEUR IA ACTIVÉ (Power BI sur Port 5433)")
print("📡 En attente de données Kafka...\n")

query = predictions_df.writeStream \
    .foreachBatch(process_batch) \
    .trigger(processingTime='10 seconds') \
    .start()

query.awaitTermination()
