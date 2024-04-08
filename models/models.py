import os
import nltk
import pandas as pd
from common import read_gbq, to_gbq, googleAPI_json_path
from dataWrangling.dataWrangling import cleanDataset, cleanGoogleMainScraped_table_name
from google.cloud import bigquery, gspread, ServiceAccountCredentials
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gensim.models.doc2vec import Doc2Vec, TaggedDocument
from nltk.tokenize import word_tokenize
from pyspark.sql.functions import split, lower, concat, col, lit
nltk.download('punkt')

# Hard-coded variables
modelDataset = "dev_modelData"
modelGoogleScraped_table_name = 'modelGoogleMain' # TODO CHANGE PATH
googleRecommendationModel_table_name = 'modelGoogleRecommendation'

# Define the scope of the Google Sheets API
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

# Authenticate with Google Sheets API using the service account key
credentials = ServiceAccountCredentials.from_json_keyfile_name(googleAPI_json_path, scope)
gspread_client = gspread.authorize(credentials)

# Read Google Sheet
sheet_url = 'https://docs.google.com/spreadsheets/d/1zo96WvtgcfznAmSjlQJpnbKIX_NfSIMpsdLcrJOYctw/edit#gid=0'
spreadsheet = gspread_client.open_by_url(sheet_url)
worksheet = spreadsheet.sheet1
data = worksheet.get_all_records()
newApplications_df = pd.DataFrame(data)

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

def appleRecommendationModel(spark, project_id, client):
    return

def googleRecommendationModel(spark, project_id, client):

    recommendationModel_table_name_db_path = f"{project_id}.{modelDataset}.{googleRecommendationModel_table_name}"
    sparkDf = read_gbq(spark, cleanDataset, cleanGoogleMainScraped_table_name)
    sparkDf = sparkDf.limit(20000)

    # Create "text" column by concatenating title, description, and summary
    sparkDf = sparkDf.withColumn('text', concat(col('title'), lit(' '), col('description'), lit(' '), col('summary')))
    # Tokenize the text and store it in the "text_tokens" column
    sparkDf = sparkDf.withColumn("text_tokens", split(lower("text"), "\s+"))
    # Select only the "text_tokens" column and collect it into a list
    text_tokens = sparkDf.select("text_tokens").rdd.flatMap(lambda x: x).collect()

    # Load data into model
    tagged_data = [TaggedDocument(d, [i]) for i, d in enumerate(text_tokens)]
    model = Doc2Vec(vector_size=64, min_count=2, epochs=40)
    model.build_vocab(tagged_data)

    # Train the model using our data
    model.train(tagged_data, total_examples=model.corpus_count, epochs=40)

    #The model can be saved for future usage
    folder_path = os.getcwd().replace("\\", "/")
    googleRecModelFile_path = f"{folder_path}/models/googleRecModel.model"
    model.save(googleRecModelFile_path)

    df = pd.DataFrame(columns = ["newApp", "appRank", "appName", "appId", "appScore"])

    for index, series in newApplications_df.iterrows():

        newData = series['What is the name of your new application?'] + ' ' + \
        series['Please provide a description for your application.'] + ' ' + \
        series['Please provide a short summary for your application.']

        test_doc = word_tokenize(newData.lower())
        test_vec = model.infer_vector(test_doc)
        results = model.docvecs.most_similar(positive=[test_vec], topn=5)

        #check the results. Do they make sense?
        for i, (doc_id, similarity_score) in enumerate(results):
            title = sparkDf.select("title").collect()[doc_id][0]
            id = sparkDf.select("appId").collect()[doc_id][0]
            text = sparkDf.select("text").collect()[doc_id][0]
            row = [series['What is the name of your new application?'], i+1, title, id, similarity_score]
            df.loc[len(df)] = row

            print(f"Result {i+1}:\n")
            print(f"Score:\n{similarity_score}\n")
            print(f"Title:\n{title}\n")
            print(f"Details:\n{text}\n")
            print("-" * 50)

    client.create_table(bigquery.Table(recommendationModel_table_name_db_path), exists_ok = True)
    to_gbq(sparkDf, modelDataset, googleRecommendationModel_table_name)