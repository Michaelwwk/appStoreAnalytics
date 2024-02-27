from dataSources.dataIngestionApple import dataIngestionApple
from dataSources.dataIngestionGoogle import dataIngestionGoogle
from pyspark.sql import SparkSession
from dataSources.dateTime import dateTime
from dataWrangling.dataWrangling import dataWrangling

# Start Spark session
spark = SparkSession.builder.master("local").appName("appStoreAnalytics").config('spark.ui.port', '4050').getOrCreate()
# spark = SparkSession.builder.appName("appStoreAnalytics").config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.23.2").getOrCreate()

dataIngestionApple()
dataIngestionGoogle()
# dataWrangling()
# finalizedMLModels()
dateTime()

# Stop Spark session
spark.stop()
