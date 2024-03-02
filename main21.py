from dataWrangling.dataWrangling import dataWrangling
from models.models import finalizedMLModels
from dataSources.dateTime import dateTime
from pyspark.sql import SparkSession

# Start Spark session
spark = SparkSession.builder.master("local").appName("appStoreAnalytics").config('spark.ui.port', '4050').getOrCreate()

dataWrangling(spark)
finalizedMLModels(spark)
dateTime(spark)

# Stop Spark session
spark.stop()