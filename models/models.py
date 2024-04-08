import os
# import nltk
# nltk.download('punkt')
from common import read_gbq, to_gbq
from dataWrangling.dataWrangling import cleanDataset, cleanGoogleMainScraped_table_name
from google.cloud import bigquery
from gensim.models.doc2vec import Doc2Vec, TaggedDocument
from nltk.tokenize import word_tokenize
from pyspark.sql.functions import split, lower, concat, col

# Hard-coded variables
modelDataset = "dev_modelData"
modelGoogleScraped_table_name = 'modelGoogleMain' # TODO CHANGE PATH

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

def appleRecommenderModel(spark, project_id, client):
    return

def googleRecommenderModel(spark, project_id, client):

    modelGoogleScraped_db_path = f"{project_id}.{modelDataset}.{modelGoogleScraped_table_name}"

    sparkDf = read_gbq(spark, cleanDataset, cleanGoogleMainScraped_table_name)
    sparkDf = sparkDf.limit(100)

    # Create "text" and "text_tokens" columns
    googleMain_sparkDF = googleMain_sparkDF.withColumn('text', concat(col('title'), col('description'), col('summary')))
    googleMain_sparkDF = googleMain_sparkDF.withColumn("text_tokens", split(lower("text"), "\s+"))

    # Select only the "text_tokens" column and collect it into a list
    text_tokens = googleMain_sparkDF.select("text_tokens").rdd.flatMap(lambda x: x).collect()

    # Load data into model
    tagged_data = [TaggedDocument(d, [i]) for i, d in enumerate(text_tokens)]
    model = Doc2Vec(vector_size=64, min_count=2, epochs=40)
    model.build_vocab(tagged_data)

    # Train the model using our data
    model.train(tagged_data, total_examples=model.corpus_count, epochs=40)

    #The model can be saved for future usage
    folder_path = os.getcwd().replace("\\", "/")
    googleRecModelFile_path = f"{folder_path}/googleAPI.json"
    model.save(googleRecModelFile_path)
    




    newData = """
    This application helps you to securely store your data in a vault with a pin code. it can include photos, videos, messages, and also your own notes such as passwords.
    """
    test_doc = word_tokenize(newData.lower())

    # Load saved doc2vec model
    model= Doc2Vec.load(googleRecModelFile_path)
    test_vec = model.infer_vector(test_doc)
    results = model.docvecs.most_similar(positive=[test_vec],topn=5)

    #check the results. Do they make sense?
    for i, d in results:
        print(f"Score:\n{d}\n")
        print(f"Title:\n{sparkDf['title'][i]}\n")
        print(f"Details:\n{sparkDf['text'][i]}")
        print("-" * 50)





    # Prep data to import into GBQ