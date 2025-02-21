from google.cloud import bigquery
from config import GCP_PROJECT_ID, BIGQUERY_DATASET

def load_data(df):
    client = bigquery.Client()
    table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.final_table"
    job = client.load_table_from_dataframe(df, table_id)
    job.result()  # Attendre la fin du job
