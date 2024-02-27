from pyspark.sql import SparkSession
from dataSources.dataIngestionApple import dataIngestionApple
from dataSources.dataIngestionGoogle import dataIngestionGoogle
from dataWrangling.dataWrangling import dataWrangling
from models.models import finalizedMLModels
from dataSources.dateTime import dateTime

# Start Spark session
spark = SparkSession.builder.master("local").appName("appStoreAnalytics").config('spark.ui.port', '4050').getOrCreate()

dataIngestionApple(noOfSlices = 1, subDf = 1)
dataIngestionGoogle(noOfSlices = 1, subDf = 1)
dataWrangling()
finalizedMLModels()
dateTime()

# Stop Spark session
spark.stop()
