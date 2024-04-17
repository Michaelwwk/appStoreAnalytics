import os
import nltk
import gzip
import pickle
from common import read_gbq, to_gbq, googleAPI_json_path, bucket_name
from dataWrangling.dataWrangling import cleanGoogleMainScraped_table_name, cleanAppleMainScraped_table_name
from google.cloud import bigquery
from google.cloud import storage
from pyspark.sql.functions import explode
import pygsheets
from gensim.models.doc2vec import Doc2Vec, TaggedDocument
from nltk.tokenize import word_tokenize
from pyspark.sql.functions import split, lower, concat, concat_ws, col, lit
from pyspark.ml.feature import Tokenizer
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
nltk.download('punkt')

# Hard-coded variables
cleanDataset = "dev_cleanData"
modelDataset = "dev_modelData"
appleRecommendationModel_table_name = 'modelAppleRecommendation'
googleRecommendationModel_table_name = 'modelGoogleRecommendation'
googleSheetURL = "https://docs.google.com/spreadsheets/d/1zo96WvtgcfznAmSjlQJpnbKIX_NfSIMpsdLcrJOYctw/edit#gid=0"
appName = "What is the name of your new application?"
appDescription = "Please provide a description for your application."
appSummary = "Please provide a short summary for your application."
appStore = "Will you be publishing your application to Apple App Store, Google Play Store, or both?"
appGenre = "What genre will your application fall under?"

# Define the scope of the Google Sheets API
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

# Authenticate with Google Sheets API using the service account key
gspread_client = pygsheets.authorize(service_file = googleAPI_json_path)

# Read Google Sheet
spreadsheet = gspread_client.open_by_url(googleSheetURL)
worksheet = spreadsheet.sheet1
data = worksheet.get_all_records()

def appleClassificationModel(spark, project_id, client):
    return

def googleClassificationModel(spark, project_id, client):
    return

def recommendationModel(spark, sparkDf, apple_google, apple_google_store, text_tokens):

    # # Tokenize the text and store it in the "text_tokens" column
    # sparkDf = sparkDf.withColumn("text_tokens", split(lower("text"), "\s+"))
    # # Select only the "text_tokens" column and collect it into a list
    # text_tokens = sparkDf.select("text_tokens").rdd.flatMap(lambda x: x).collect() # this one is too computationally extensive

    # # Load data into model
    # print("Loading data into model ..")
    # tagged_data = [TaggedDocument(d, [i]) for i, d in enumerate(text_tokens)]
    # model = Doc2Vec(vector_size=64, min_count=2, epochs=40)
    # model.build_vocab(tagged_data)
    # print("Data loaded into model.")

    # # Train the model using our data
    # print("Training model ..")
    # model.train(tagged_data, total_examples=model.corpus_count, epochs=40)
    # print("Model trained.")

    # Read data from GCS & download to local
    folder_path = os.getcwd().replace("\\", "/")
    blobs = storage.Client().bucket(bucket_name).list_blobs(prefix=f"{apple_google}RecModel.model")
    path_dict = {}
    index = 0
    for blob in blobs:
        filename = os.path.basename(blob.name)
        recModelFile_path = os.path.join(folder_path, filename)
        blob.download_to_filename(recModelFile_path)
        print(f"Downloaded: {filename} to {recModelFile_path}")
        path_dict[index] = recModelFile_path
        index += 1

    # Filter for relevant rows in new applications dataset
    newApplications_df = spark.createDataFrame(data)
    newApplications_df = newApplications_df.filter(
    (newApplications_df[appStore] == apple_google_store) |
    (newApplications_df[appStore] == "Both")
    )

    # Define the schema
    schema = StructType([
        StructField("newApp", StringType(), nullable=True),
        StructField("newAppGenre", StringType(), nullable=True),
        StructField("newAppDescription", StringType(), nullable=True),
        StructField("appRank", StringType(), nullable=True),
        StructField("appName", StringType(), nullable=True),
        StructField("appId", StringType(), nullable=True),
        StructField("appScore", DoubleType(), nullable=True)
    ])

    # Create an empty DataFrame with the specified schema
    df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

    # Concatenate the columns into a single column "text"

    # if apple_google == 'google':
    #     newApplications_df = newApplications_df.withColumn(
    #         "text",
    #         concat_ws(" ", 
    #                 lower(col(appName)), 
    #                 lower(col(f"`{appDescription}`")), 
    #                 lower(col(f"`{appSummary}`")))
    #     )
    # else:
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
        results = model.docvecs.most_similar(positive=[test_vec], topn=10)
        print("[Original]\n")
        print(f"Genre: {row[appGenre]}")
        print(f"App Name: {row[appName]}")
        print(f"Description: {row[appDescription]}")
        # if apple_google == 'google':
        #     print(f"Summary: {row[appSummary]}")
        print("-" * 50)
        
        # Iterate over the results and append rows to the DataFrame
        for i, (doc_id, similarity_score) in enumerate(results):

            row = sparkDf.where(col('_c0').isin(doc_id))
            genre = row.select("genre").collect()[0][0]
            title = row.select("title").collect()[0][0]
            description = row.select("description").collect()[0][0]
            id = row.select("appId").collect()[0][0]

            # genre = sparkDf.select("genre").collect()[doc_id][0]
            # title = sparkDf.select("title").collect()[doc_id][0]
            # # description = sparkDf.select("description").collect()[doc_id][0]
            # # summary = sparkDf.select("summary").collect()[doc_id][0]
            # id = sparkDf.select("appId").collect()[doc_id][0]

            print(f"[Result {i+1}]\n")
            print(f"Genre:\n{genre}\n")
            print(f"App Name:\n{title}\n")
            print(f"Description:\n{description}\n")
            # if apple_google == 'google':
            #     print(f"Summary:\n{summary}\n")
            print(f"Score:\n{similarity_score}\n")
            print("-" * 50)
            
            # Create a new row
            new_row = (row[appName], row[appGenre], row[appDescription], str(i+1), title, id, similarity_score)
            
            # Append the row to the DataFrame
            df = df.union(spark.createDataFrame([new_row], schema=schema))

    # Filter out the empty row
    df = df.filter(df.newApp.isNotNull())

    # Remove paths in local storage
    for path in path_dict.values():
        os.remove(path)

    return df

def appleRecommendationModel(spark, project_id, client):

    sparkDf = read_gbq(spark, cleanDataset, cleanAppleMainScraped_table_name)
    # sparkDf = sparkDf.withColumn('text', concat(col('title'), lit(' '), col('description')))
    # sparkDf = sparkDf.orderBy(col("appId")).drop("index")
    print("Apple sparkDf loaded.")
    print(sparkDf.count())

    # for model training purposes only
    pandasDf = read_gbq(spark, cleanDataset, cleanAppleMainScraped_table_name, sparkDf = False)
    # pandasDf.sort_values(by = 'appId').reset_index(drop = True)
    print("Apple pandasDf loaded.")
    print(pandasDf.shape)
    pandasDf['textonly'] = pandasDf['title'] + ' ' + pandasDf['description']
    textonly = pandasDf['textonly'].fillna('')
    text_tokens = [word_tokenize(t.lower()) for t in textonly]

    df = recommendationModel(spark, sparkDf, apple_google = 'apple', apple_google_store = 'Apple App Store', text_tokens = text_tokens)
    recommendationModel_table_name_db_path = f"{project_id}.{modelDataset}.{appleRecommendationModel_table_name}"
    client.create_table(bigquery.Table(recommendationModel_table_name_db_path), exists_ok = True)
    to_gbq(df, modelDataset, appleRecommendationModel_table_name)

def googleRecommendationModel(spark, project_id, client):
    
    sparkDf = read_gbq(spark, cleanDataset, cleanGoogleMainScraped_table_name)
    # sparkDf = sparkDf.withColumn('text', concat(col('title'), lit(' '), col('description'), lit(' '), col('summary')))
    # sparkDf = sparkDf.orderBy(col("appId")).drop("index")
    print("Google sparkDf loaded.")
    print(sparkDf.count())

    # for model training purposes only
    pandasDf = read_gbq(spark, cleanDataset, cleanGoogleMainScraped_table_name, sparkDf = False)
    # pandasDf.sort_values(by = 'appId').reset_index(drop = True)
    print("Google pandasDf loaded.")
    print(pandasDf.shape)
    # pandasDf['textonly'] = pandasDf['title'] + ' ' + pandasDf['description'] + pandasDf['summary']
    pandasDf['textonly'] = pandasDf['title'] + ' ' + pandasDf['description']
    textonly = pandasDf['textonly'].fillna('')
    text_tokens = [word_tokenize(t.lower()) for t in textonly]

    df = recommendationModel(spark, sparkDf, apple_google = 'google', apple_google_store = 'Google Play Store', text_tokens = text_tokens)
    recommendationModel_table_name_db_path = f"{project_id}.{modelDataset}.{googleRecommendationModel_table_name}"
    client.create_table(bigquery.Table(recommendationModel_table_name_db_path), exists_ok = True)
    to_gbq(df, modelDataset, googleRecommendationModel_table_name)
