from dataSources.dataIngestion import dataIngestion
from pyspark.sql import SparkSession

# Start Spark session
spark = SparkSession.builder.master("local").appName("appStoreAnalytics").config('spark.ui.port', '4050').getOrCreate()

dataIngestion(spark)

# Stop Spark session
spark.stop()
