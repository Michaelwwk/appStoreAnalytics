from pyspark.sql import SparkSession
from dataWrangling.dataWrangling import dataWrangling
from models.models import finalizedMLModels
from dataSources.dateTime import dateTime

# Start Spark session
spark = SparkSession.builder.appName("appStoreAnalytics").getOrCreate()

dataWrangling()
finalizedMLModels()
dateTime()

# Stop Spark session
spark.stop()