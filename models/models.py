import os
import nltk
import gzip
import pickle
import pandas as pd
from common import read_gbq, to_gbq, googleAPI_json_path, bucket_name
from dataWrangling.dataWrangling import cleanDataset, cleanGoogleMainScraped_table_name, cleanAppleMainScraped_table_name
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
modelDataset = "prod_modelData"
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

def recommendationModel(pandasDf, apple_google_store):

    # Filter for relevant rows in new applications dataset
    newApplications_df = pd.DataFrame(data)
    newApplications_df = newApplications_df[(newApplications_df[appStore] == apple_google_store) | (newApplications_df[appStore] == "Both")]

    # Filter for rows only in the genre of the new app!!
    # newAppsGenre_set = set(newApplications_df[appGenre])
    # pandasDf = pandasDf[pandasDf["genre"].isin(newAppsGenre_set)]

    # Load data into model
    print("Loading data into model ..")
    pandasDf['textonly'] = pandasDf['title'] + ' ' + pandasDf['description']
    textonly = pandasDf['textonly'].fillna('')
    text_tokens = [word_tokenize(t.lower()) for t in textonly]
    tagged_data = [TaggedDocument(d, [i]) for i, d in enumerate(text_tokens)]
    model = Doc2Vec(vector_size=64, min_count=2, epochs=40)
    model.build_vocab(tagged_data)
    print("Data loaded into model.")

    # Train the model using our data
    print("Training model ..")
    model.train(tagged_data, total_examples=model.corpus_count, epochs=40)
    print("Model trained.")

    # # Read data from GCS & download to local
    # folder_path = os.getcwd().replace("\\", "/")
    # blobs = storage.Client().bucket(bucket_name).list_blobs(prefix=f"{apple_google}RecModel.model") # to fill either 'apple' or 'google'!!
    # path_dict = {}
    # index = 0
    # for blob in blobs:
    #     filename = os.path.basename(blob.name)
    #     recModelFile_path = os.path.join(folder_path, filename)
    #     blob.download_to_filename(recModelFile_path)
    #     print(f"Downloaded: {filename} to {recModelFile_path}")
    #     path_dict[index] = recModelFile_path
    #     index += 1

    # # Load saved doc2vec model
    # model = Doc2Vec.load(path_dict[0])

    df = pd.DataFrame(columns = ["newApp", "newAppGenre", "newAppDescription", "appRank", "appName", "appId", "appScore"])

    # Iterate over each row and compute similarity scores
    for index, series in newApplications_df.iterrows():

        newData = series[appName] + ' ' + series[appDescription]
        test_doc = word_tokenize(newData.lower())
        test_vec = model.infer_vector(test_doc)
        results = model.docvecs.most_similar(positive=[test_vec], topn=10)
        print("[Original]\n")
        print(f"Genre: {series[appGenre]}")
        print(f"App Name: {series[appName]}")
        print(f"Description: {series[appDescription]}")
        print("-" * 50)

        # Iterate over the results and append rows to the DataFrame
        for i, (index, similarity_score) in enumerate(results):

            genre = pandasDf["genre"][index]
            title = pandasDf["title"][index]
            id = pandasDf["appId"][index]
            description = pandasDf["description"][index]

            print(f"[Result {i+1}]\n")
            print(f"Genre:\n{genre}\n")
            print(f"App Name:\n{title}\n")
            print(f"App ID:\n{id}\n")
            print(f"Description:\n{description}\n")
            print(f"Score:\n{similarity_score}\n")
            print("-" * 50)

            row = [series[appName], series[appGenre], series[appDescription], str(i+1), title, id, similarity_score]
            df.loc[len(df)] = row

    # # Remove paths in local storage
    # for path in path_dict.values():
    #     os.remove(path)

    print(df)

    return df

def appleRecommendationModel(spark, project_id, client):

    recommendationModel_table_name_db_path = f"{project_id}.{modelDataset}.{appleRecommendationModel_table_name}"
    pandasDf = read_gbq(spark, cleanDataset, cleanAppleMainScraped_table_name, sparkDf = False)
    print("Apple pandasDf loaded.")

    df = recommendationModel(pandasDf, apple_google_store = 'Apple App Store')
    client.create_table(bigquery.Table(recommendationModel_table_name_db_path), exists_ok = True)
    to_gbq(df, modelDataset, appleRecommendationModel_table_name, sparkDf = False)

def googleRecommendationModel(spark, project_id, client):

    recommendationModel_table_name_db_path = f"{project_id}.{modelDataset}.{googleRecommendationModel_table_name}"
    pandasDf = read_gbq(spark, cleanDataset, cleanGoogleMainScraped_table_name, sparkDf = False)
    print("Google pandasDf loaded.")

    df = recommendationModel(pandasDf, apple_google_store = 'Google Play Store')
    client.create_table(bigquery.Table(recommendationModel_table_name_db_path), exists_ok = True)
    to_gbq(df, modelDataset, googleRecommendationModel_table_name, sparkDf = False)