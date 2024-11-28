from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import os

# Initialisation de SparkSession
spark = SparkSession.builder \
    .appName("DataLakeAPI") \
    .getOrCreate()

# Chemins HDFS pour la couche Gold
hdfs_gold_path = "hdfs://localhost:9000/datalake/gold"

class TotalSalesPerProductView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        gold_path = os.path.join(hdfs_gold_path, "total_sales_per_product")
        try:
            df = spark.read.parquet(gold_path)
            data = [row.asDict() for row in df.collect()]
            return Response(data)
        except Exception as e:
            return Response({"error": f"Erreur lors de la lecture des ventes par produit : {e}"}, status=500)


class StatusCodeCountsView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        gold_path = os.path.join(hdfs_gold_path, "status_code_counts")
        try:
            df = spark.read.parquet(gold_path)
            data = [row.asDict() for row in df.collect()]
            return Response(data)
        except Exception as e:
            return Response({"error": f"Erreur lors de la lecture des codes de statut HTTP : {e}"}, status=500)


class AvgLikesPerProductView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        gold_path = os.path.join(hdfs_gold_path, "avg_likes_per_product")
        try:
            df = spark.read.parquet(gold_path)
            data = [row.asDict() for row in df.collect()]
            return Response(data)
        except Exception as e:
            return Response({"error": f"Erreur lors de la lecture des likes moyens par produit : {e}"}, status=500)


class CampaignMetricsView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        gold_path = os.path.join(hdfs_gold_path, "campaign_metrics")
        try:
            df = spark.read.parquet(gold_path)
            data = [row.asDict() for row in df.collect()]
            return Response(data)
        except Exception as e:
            return Response({"error": f"Erreur lors de la lecture des métriques des campagnes : {e}"}, status=500)
