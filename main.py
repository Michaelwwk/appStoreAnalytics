from dataSources.dataIngestion import dataIngestion
from pyspark.sql import SparkSession

# Start Spark session
# spark = SparkSession.builder.master("local").appName("appStoreAnalytics").config('spark.ui.port', '4050').getOrCreate()
spark = SparkSession.builder.appName("appStoreAnalytics").config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.23.2").getOrCreate()

dataIngestion(spark)

# Stop Spark session
spark.stop()
