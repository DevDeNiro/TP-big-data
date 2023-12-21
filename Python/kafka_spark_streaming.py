from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, when, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, FloatType, TimestampType, IntegerType
import os

# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.hadoop:hadoop-aws:3.2.0 pyspark-shell'
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.hadoop:hadoop-aws:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell'


# --------------------------------PARTIE 1------------------------------------ #
# initialisation de SparkSession + configuration pour Minio
spark_session = SparkSession.builder \
    .appName("KafkaToSparkStreaming") \
    .master("local[*]") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.563") \
    .getOrCreate()

spark_session.sparkContext.setLogLevel("ERROR")

# --------------------------------PARTIE 2------------------------------------ #

spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0") \
    .getOrCreate()

# Définition du schéma des données Kafka
schema = StructType().add("id_transaction", StringType()) \
    .add("type_transaction", StringType()) \
    .add("montant", FloatType()) \
    .add("devise", StringType()) \
    .add("date", TimestampType()) \
    .add("lieu", StringType()) \
    .add("moyen_paiement", StringType()) \
    .add("details", StructType()
         .add("produit", StringType()) \
         .add("quantite", IntegerType()) \
         .add("prix_unitaire", FloatType())) \
    .add("utilisateur", StructType()
         .add("id_utilisateur", StringType()) \
         .add("nom", StringType()) \
         .add("adresse", StringType()) \
         .add("email", StringType()))

# Lire depuis Kafka Conduktor + conversion en DataFrame au format JSON
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", "tp-big-data") \
    .load()

df = kafka_df.selectExpr("CAST(value AS STRING)").select(from_json("value", schema).alias("data")).select("data.*")

# --------------------------------------------------------------------
#                     MANIP SUR LES DF
# --------------------------------------------------------------------

# Traitement des données
df = df.withColumn("devise", when(col("devise") == "USD", "EUR").otherwise(col("devise")))
# df = df.withColumn("montant_eur", col("montant") * 0.85)  # Conversion USD en EUR (exemple de taux de change)
# df = df.withColumn("date_timestamp", to_timestamp(col("date"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))  # Ajouter le TimeZone
# df = df.withColumn("date", to_date(col("date")))  # Convertir la date en string en une valeur date
# df = df.filter(col("moyen_paiement") != "erreur")  # Supprimer les transactions en erreur
# df = df.filter(col("lieu").isNotNull())  # Supprimer les valeurs None pour l'adresse

query = (df.writeStream.outputMode("append")
         .format("parquet")
         .option("checkpointLocation", "metadata")
         .option("path", "elem").start())

query.awaitTermination()

# --------------------------------PARTIE 3------------------------------------ #

# Écrire le DataFrame au format .parquet dans Minio
output_path = "s3a://warehouse/parquet/"

# Écrire le DataFrame en streaming
query = df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("checkpointLocation", "/warehouse") \
    .option("path", output_path) \
    .start()

# Arret de la session d'écriture, donc du flux en streaming
query.awaitTermination()

# -------------------------------PARTIE 4------------------------------------- #

# Lire le DataFrame au format .parquet depuis Minio
df_read = spark_session.read.options(inferSchema="true", delimiter=",", header="true") \
    .parquet("elem")

df_read.show()
df_read.printSchema()
