from google.cloud import bigquery

def to_gbq(pandasDf, client, dataSet_tableName):
    df = pandasDf.copy()
    job_config = bigquery.LoadJobConfig(write_disposition='WRITE_TRUNCATE')
    load_job = client.load_table_from_dataframe(
        df,
        dataSet_tableName,
        job_config=job_config
    )
    return load_job