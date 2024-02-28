from pyspark.sql import SparkSession
from dataWrangling.dataWrangling import dataWrangling
from models.models import finalizedMLModels
from dataSources.dateTime import dateTime

# Start Spark session
spark = SparkSession.builder.master("local").appName("appStoreAnalytics").config('spark.ui.port', '4050').getOrCreate()

dataWrangling()
finalizedMLModels()
dateTime()

# Stop Spark session
spark.stop()