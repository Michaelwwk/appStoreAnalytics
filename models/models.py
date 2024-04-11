import os
import nltk
import gzip
import pickle
from common import read_gbq, to_gbq, googleAPI_json_path
from dataWrangling.dataWrangling import cleanDataset, cleanGoogleMainScraped_table_name, cleanAppleMainScraped_table_name
from google.cloud import bigquery
import pygsheets
from gensim.models.doc2vec import Doc2Vec, TaggedDocument
from nltk.tokenize import word_tokenize
from pyspark.sql.functions import split, lower, concat, concat_ws, col, lit
from pyspark.ml.feature import Tokenizer
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
nltk.download('punkt')

# Hard-coded variables
modelDataset = "dev_modelData"
modelAppleScraped_table_name = 'modelAppleMain' # TODO just for example!
modelGoogleScraped_table_name = 'modelGoogleMain' # TODO just for example!
appleRecommendationModel_table_name = 'modelAppleRecommendation'
googleRecommendationModel_table_name = 'modelGoogleRecommendation'
googleSheetURL = "https://docs.google.com/spreadsheets/d/1zo96WvtgcfznAmSjlQJpnbKIX_NfSIMpsdLcrJOYctw/edit#gid=0"
appName = "What is the name of your new application?"
appDescription = "Please provide a description for your application."
appSummary = "Please provide a short summary for your application."
appStore = "Will you be publishing your application to Apple App Store, Google Play Store, or both?"

# Define the scope of the Google Sheets API
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

# Authenticate with Google Sheets API using the service account key
gspread_client = pygsheets.authorize(service_file = googleAPI_json_path)

# Read Google Sheet
spreadsheet = gspread_client.open_by_url(googleSheetURL)
worksheet = spreadsheet.sheet1
data = worksheet.get_all_records()

# TODO Follow this template when scripting!!
def appleClassificationModel(spark, project_id, client):
    return

def googleClassificationModel(spark, project_id, client):
    
    modelGoogleScraped_db_path = f"{project_id}.{modelDataset}.{modelGoogleScraped_table_name}"

    sparkDf = read_gbq(spark, cleanDataset, cleanGoogleMainScraped_table_name)
    # print(sparkDf.show())
    print(sparkDf.count())

    client.create_table(bigquery.Table(modelGoogleScraped_db_path), exists_ok = True)
    to_gbq(sparkDf, modelDataset, modelGoogleScraped_table_name)

def recommendationModel(spark, sparkDf, apple_google, apple_google_store):

    folder_path = os.getcwd().replace("\\", "/")





    

    # Import libraries
    from google.cloud import storage
    import os

    # Replace with your bucket name
    bucket_name = "nusebac_storage"

    # Replace with the path to your model file in GCS
    model_file_path = f'gs://{bucket_name}/{apple_google}RecModel.model'

    # Create a GCS client
    client = storage.Client()

    # Get a reference to the model directory (assuming the model files are in the same directory)
    model_dir = os.path.dirname(model_file_path)

    # List all blobs in the directory
    blobs = client.bucket(bucket_name).list_blobs(prefix=model_dir)

    path_dict = {}
    index = 0
    for blob in blobs:
        # Extract filename from the blob object
        filename = os.path.basename(blob.name)

        # Construct a local file path with a descriptive name
        recModelFile_path = os.path.join(folder_path, filename)

        # Download the blob to the local file path
        blob.download_to_filename(recModelFile_path)
        print(f"Downloaded: {filename} to {recModelFile_path}")

        path_dict[index] = recModelFile_path
        index += 1

        







    newApplications_df = spark.createDataFrame(data)
    newApplications_df = newApplications_df.filter(
    (newApplications_df[appStore] == apple_google_store) |
    (newApplications_df[appStore] == "Both")
    )

    # Define the schema
    schema = StructType([
        StructField("newApp", StringType(), nullable=True),
        StructField("appRank", StringType(), nullable=True),
        StructField("appName", StringType(), nullable=True),
        StructField("appId", StringType(), nullable=True),
        StructField("appScore", DoubleType(), nullable=True)
    ])

    # Create an empty DataFrame with the specified schema
    df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

    # Concatenate the columns into a single column "text"

    if apple_google == 'google':
        newApplications_df = newApplications_df.withColumn(
            "text",
            concat_ws(" ", 
                    lower(col(appName)), 
                    lower(col(f"`{appDescription}`")), 
                    lower(col(f"`{appSummary}`")))
        )
    else:
        newApplications_df = newApplications_df.withColumn(
            "text",
            concat_ws(" ", 
                    lower(col(appName)), 
                    lower(col(f"`{appDescription}`")) 
                    )
        )

    # Tokenize the text column
    tokenizer = Tokenizer(inputCol="text", outputCol="text_tokens")
    newApplications_df = tokenizer.transform(newApplications_df)

    # Load saved doc2vec model
    model = Doc2Vec.load(path_dict[0])

    # Iterate over each row and compute similarity scores
    for row in newApplications_df.collect():
        test_vec = model.infer_vector(row["text_tokens"])
        results = model.docvecs.most_similar(positive=[test_vec], topn=5)
        print("[Original]\n")
        print(f"Title: {row[appName]}")
        print(f"Description: {row[appDescription]}")
        if apple_google == 'google':
            print(f"Summary: {row[appSummary]}")
        print("-" * 50)
        
        # Iterate over the results and append rows to the DataFrame
        for i, (doc_id, similarity_score) in enumerate(results):

            title = sparkDf.select("title").collect()[doc_id][0]
            id = sparkDf.select("appId").collect()[doc_id][0]
            text = sparkDf.select("text").collect()[doc_id][0]
            print(f"[Result {i+1}]\n")
            print(f"Score:\n{similarity_score}\n")
            print(f"Title:\n{title}\n")
            print(f"Details:\n{text}\n")
            print("-" * 50)
            
            # Create a new row
            new_row = (row[appName], str(i+1), title, id, similarity_score)
            
            # Append the row to the DataFrame
            df = df.union(spark.createDataFrame([new_row], schema=schema))

    # Filter out the empty row
    df = df.filter(df.newApp.isNotNull())

    # Remove paths in local storage
    for path in path_dict.values():
        os.remove(path)

    return df

def appleRecommendationModel(spark, project_id, client):

    recommendationModel_table_name_db_path = f"{project_id}.{modelDataset}.{appleRecommendationModel_table_name}"
    sparkDf = read_gbq(spark, cleanDataset, cleanAppleMainScraped_table_name)
    print("Apple sparkDf loaded.")

    sparkDf = sparkDf.withColumn('text', concat(col('title'), lit(' '), col('description')))
    df = recommendationModel(spark, sparkDf, apple_google = 'apple', apple_google_store = 'Apple App Store')
    
    client.create_table(bigquery.Table(recommendationModel_table_name_db_path), exists_ok = True)
    to_gbq(df, modelDataset, appleRecommendationModel_table_name)

def googleRecommendationModel(spark, project_id, client):

    recommendationModel_table_name_db_path = f"{project_id}.{modelDataset}.{googleRecommendationModel_table_name}"
    sparkDf = read_gbq(spark, cleanDataset, cleanGoogleMainScraped_table_name)
    print("Google sparkDf loaded.")

    sparkDf = sparkDf.withColumn('text', concat(col('title'), lit(' '), col('description'), lit(' '), col('summary')))
    df = recommendationModel(spark, sparkDf, apple_google = 'google', apple_google_store = 'Google Play Store')

    client.create_table(bigquery.Table(recommendationModel_table_name_db_path), exists_ok = True)
    to_gbq(df, modelDataset, googleRecommendationModel_table_name)