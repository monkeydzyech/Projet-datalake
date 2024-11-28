from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, regexp_extract, when
from pyspark.sql.types import IntegerType

# Initialisation de SparkSession
spark = SparkSession.builder \
    .appName("Silver_Transformation") \
    .getOrCreate()

# Chemins HDFS
bronze_path = "hdfs://localhost:9000/datalake/bronze/"
silver_path = "hdfs://localhost:9000/datalake/silver/"

# =====================
# 1. Transactions clients
# =====================
df_transactions = spark.read.csv(f"{bronze_path}/transactions", header=True, inferSchema=True)
df_transactions_clean = df_transactions \
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
    .filter(col("date").isNotNull()) \
    .withColumn("quantite", when(col("quantite") > 0, col("quantite")).otherwise(0)) \
    .withColumn("prix", when(col("prix").isNotNull(), col("prix")).otherwise(0.0))

df_transactions_clean.write.parquet(f"{silver_path}/transactions", mode="append")

# =====================
# 2. Logs des serveurs web
# =====================
df_logs = spark.read.text(f"{bronze_path}/server_logs")
log_pattern = r'(\d{1,3}(?:\.\d{1,3}){3}) - - \[(.*?)\] "(.*?)" (\d{3}) (\d+)?'
df_logs_clean = df_logs \
    .filter(regexp_extract('value', log_pattern, 0) != "") \
    .select(
        regexp_extract('value', log_pattern, 1).alias('ip'),
        regexp_extract('value', log_pattern, 2).alias('timestamp'),
        regexp_extract('value', log_pattern, 3).alias('request'),
        regexp_extract('value', log_pattern, 4).cast(IntegerType()).alias('status_code'),
        regexp_extract('value', log_pattern, 5).cast(IntegerType()).alias('response_size')
    )

df_logs_clean.write.parquet(f"{silver_path}/server_logs", mode="append")

# =====================
# 3. Données des médias sociaux
# =====================
df_social = spark.read.json(f"{bronze_path}/social_media")
df_social_clean = df_social \
    .filter(col("contenu").isNotNull()) \
    .withColumn("likes", when(col("likes").cast(IntegerType()).isNotNull(), col("likes").cast(IntegerType())).otherwise(0)) \
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
    .filter(col("date").isNotNull())

df_social_clean.write.parquet(f"{silver_path}/social_media", mode="append")

# =====================
# 4. Flux publicitaire
# =====================
df_pub_stream = spark.read.json(f"{bronze_path}/pub_stream")
df_pub_stream_clean = df_pub_stream \
    .withColumn("clicks", when(col("clicks").isNotNull(), col("clicks")).otherwise(0)) \
    .withColumn("impressions", when(col("impressions").isNotNull(), col("impressions")).otherwise(0)) \
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
    .filter(col("date").isNotNull())

df_pub_stream_clean.write.parquet(f"{silver_path}/pub_stream", mode="append")

print("Nettoyage des données Silver terminé.")
