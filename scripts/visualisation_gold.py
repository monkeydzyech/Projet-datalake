from pyspark.sql import SparkSession

# Initialisation de SparkSession
spark = SparkSession.builder \
    .appName("Visualisation Gold") \
    .getOrCreate()

# Chemins des fichiers Parquet dans Gold
gold_path = "hdfs://localhost:9000/datalake/gold/"

# Lire les fichiers Parquet pour chaque table Gold
df_avg_likes_per_product = spark.read.parquet(f"{gold_path}/avg_likes_per_product")
df_avg_likes_per_product.show(truncate=False)

df_campaign_metrics = spark.read.parquet(f"{gold_path}/campaign_metrics")
df_campaign_metrics.show(truncate=False)

df_status_code_counts = spark.read.parquet(f"{gold_path}/status_code_counts")
df_status_code_counts.show(truncate=False)

df_total_sales_per_product = spark.read.parquet(f"{gold_path}/total_sales_per_product")
df_total_sales_per_product.show(truncate=False)

# Fermer la session Spark
spark.stop()
