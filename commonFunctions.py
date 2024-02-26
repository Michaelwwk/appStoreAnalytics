from google.cloud import bigquery

def to_gbq(pandasDf, client, dataSet_tableName, mergeType ='WRITE_TRUNCATE'):
    df = pandasDf.copy()
    job_config = bigquery.LoadJobConfig(write_disposition=mergeType)
    load_job = client.load_table_from_dataframe(
        df,
        dataSet_tableName,
        job_config=job_config
    )
    return load_job