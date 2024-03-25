import nltk
import string
import pandas as pd
from common import read_gbq, to_gbq
from google.cloud import bigquery
from dataWrangling.dataWrangling import cleanDataset, cleanGoogleMainScraped_table_name
from nltk import word_tokenize, FreqDist
from nltk.corpus import stopwords
from gensim import corpora, models
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import TruncatedSVD
import shutil
import time

# Hard-coded variables
modelDataset = "modelData"
modelGoogleScraped_table_name = 'modelGoogleMain' # TODO CHANGE PATH

trainTestDataset = 'trainTestData'
modelAppleleReview_table_name = 'appleReview'
modelGoogleReview_table_name = 'googleReview'

# TODO Follow this template when scripting!!
def finalizedMLModels(spark, project_id, client):

    # modelGoogleScraped_db_path = f"{project_id}.{modelDataset}.{modelGoogleScraped_table_name}"

    appleReview = read_gbq(spark, trainTestDataset, modelAppleleReview_table_name)
    googleReview = read_gbq(spark, trainTestDataset, modelGoogleReview_table_name)

    local_file_path = f"{modelAppleleReview_table_name}.parquet"
    appleReview.write.parquet(local_file_path, mode="overwrite")
    appleReview = pd.read_parquet(local_file_path)
    try:
        shutil.rmtree(local_file_path)
    except:
        pass

    local_file_path = f"{modelGoogleReview_table_name}.parquet"
    googleReview.write.parquet(local_file_path, mode="overwrite")
    googleReview = pd.read_parquet(local_file_path)
    try:
        shutil.rmtree(local_file_path)
    except:
        pass

    def preprocess_text(tokens):

        # variable "tokens" being a list of words/tokens/characters
        # Convert all characters to lower case
        tokens = [t.lower() for t in tokens]
        # Remove Punctuations (.,)
        tokens = [t for t in tokens if t not in string.punctuation]
        # Remove Stopwords (I, you, our, we)
        stop = stopwords.words('english')
        tokens = [t for t in tokens if t not in stop]
        # Remove Numbers/Numerics
        tokens = [t for t in tokens if not t.isnumeric()]
        # Remove from filtered list, to manually include
        filter_list = ["’", "“", "”", "would", "could", "'s", "left", "right", "a.m.", "p.m."]
        tokens = [t for t in tokens if ':' not in t and t not in filter_list]

        # Lemmatization - Reduce words to base form/lemma
        #nltk.download('omw-1.4')
        wnl = nltk.WordNetLemmatizer()
        tokens = [ wnl.lemmatize(t) for t in tokens ] 

        return tokens
    
    for store in [appleReview.head(50), googleReview.head(50)]:

        # Record the start time
        start_time = time.time()

        documents = store['content'].apply(preprocess_text)
        dictionary = corpora.Dictionary(documents)
        dictionary.filter_extremes(no_below=2, no_above=0.8)
        corpus = [dictionary.doc2bow(doc) for doc in documents]
        tfidf = models.TfidfModel(corpus)
        tfidf_corpus = tfidf[corpus]
        print(store)
        print("tfidf_corpus:")
        print(tfidf_corpus)

        try:
            n_components = 1000  # This no. must be the same or smaller than the no. of rows!! Typically set between 100 to 300
            svd = TruncatedSVD(n_components=n_components)
            tfidf_reduced = svd.fit_transform(tfidf_corpus)
            tfidf_df = pd.DataFrame(tfidf_reduced, columns=[f'component_{i}' for i in range(n_components)])
            print(f"tfidf_corpus successfully converted to tfidf_df! Shape: {tfidf_df.shape}")
        except:
            print("tfidf_corpus can't be converted into tfidf_df!")

         # Record the end time
        end_time = time.time()         
        # Calculate and print the elapsed time in seconds
        elapsed_time = end_time - start_time
        print(f"Time taken for tfidf_corpus: {elapsed_time}")

        # Record the start time
        start_time = time.time()

        tdm = TfidfVectorizer(tokenizer = preprocess_text, min_df = 2, max_df = 0.7)
        tfidf_dtm = tdm.fit_transform(store['content'])
        print(store)
        print("tfidf_dtm:")
        print(tfidf_dtm)

        try:
            n_components = 1000  # This no. must be the same or smaller than the no. of rows!! Typically set between 100 to 300
            svd = TruncatedSVD(n_components=n_components)
            tfidf_reduced = svd.fit_transform(tfidf_dtm)
            tfidf_df = pd.DataFrame(tfidf_reduced, columns=[f'component_{i}' for i in range(n_components)])
            print(f"tfidf_dtm successfully converted to tfidf_df! Shape: {tfidf_df.shape}")
        except:
            print("tfidf_dtm can't be converted into tfidf_df!")

         # Record the end time
        end_time = time.time()         
        # Calculate and print the elapsed time in seconds
        elapsed_time = end_time - start_time
        print(f"Time taken for tfidf_dtm: {elapsed_time}")


        











    

    # client.create_table(bigquery.Table(modelGoogleScraped_db_path), exists_ok = True)
    # to_gbq(sparkDf, modelDataset, modelGoogleScraped_table_name)
