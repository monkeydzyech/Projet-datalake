from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Initialisation de SparkSession
spark = SparkSession.builder \
    .appName("Bronze_Ingestion") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

# Chemins HDFS
hdfs_bronze_path = "hdfs://localhost:9000/datalake/bronze/"

# Chemins locaux des fichiers
local_transactions_path = "file:///Users/monkeydziyech/Desktop/datalake_medaillon/data/transaction.csv"
local_logs_path = "file:///Users/monkeydziyech/Desktop/datalake_medaillon/data/logs.txt"
local_social_path = "file:///Users/monkeydziyech/Desktop/datalake_medaillon/data/media.json"

# =====================
# Étape 1 : Transactions clients (CSV local)
# =====================
transactions_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("client_id", IntegerType(), True),
    StructField("produit", StringType(), True),
    StructField("quantite", IntegerType(), True),
    StructField("prix", DoubleType(), True),
    StructField("date", StringType(), True)
])

df_transactions = spark.read.csv(local_transactions_path, schema=transactions_schema, header=True)
df_transactions.write.csv(f"{hdfs_bronze_path}/transactions", header=True, mode="append")
print("Transactions clients ingérées dans la couche Bronze.")

# =====================
# Étape 2 : Logs des serveurs web (Texte local)
# =====================
df_logs = spark.read.text(local_logs_path)

# Append des nouvelles logs
df_logs.write.format("text").mode("append").save(f"{hdfs_bronze_path}/server_logs")
print("Logs des serveurs web ingérés dans la couche Bronze.")

# =====================
# Étape 3 : Données des médias sociaux (JSON local)
# =====================
df_social = spark.read.json(local_social_path)
df_social.write.json(f"{hdfs_bronze_path}/social_media", mode="append")
print("Données des médias sociaux ingérées dans la couche Bronze.")

# =====================
# Étape 4 : Flux publicitaire (Consommation Kafka)
# =====================
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "publicite_stream"

streaming_schema = StructType([
    StructField("campaign_id", StringType(), True),
    StructField("clicks", IntegerType(), True),
    StructField("impressions", IntegerType(), True),
    StructField("date", StringType(), True)
])

df_kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

df_stream = df_kafka_stream.selectExpr("CAST(value AS STRING) as json_value")
df_parsed = df_stream.withColumn("data", from_json(col("json_value"), streaming_schema)).select("data.*")

query = df_parsed.writeStream \
    .format("json") \
    .option("path", f"{hdfs_bronze_path}/pub_stream") \
    .option("checkpointLocation", "/tmp/checkpoint/bronze_pub") \
    .outputMode("append") \
    .start()

query.awaitTermination()
