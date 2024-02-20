from google.cloud import bigquery

def to_gbq(pandasDf, client, destination_table_id):
    df = pandasDf.copy()
    job_config = bigquery.LoadJobConfig(write_disposition='WRITE_TRUNCATE')
    load_job = client.load_table_from_dataframe(
        df,
        destination_table_id,
        job_config=job_config
    )
    return load_job