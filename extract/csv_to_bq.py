import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound

service_account_path = '../service_account.json'
credentials = service_account.Credentials.from_service_account_file(service_account_path)
bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)


def upload_csv_to_bigquery(csv_file_path, dataset_id, table_id):
    """
    Uploads a CSV file to a specified BigQuery table.

    Parameters:
    csv_file_path (str): The file path to the CSV file.
    dataset_id (str): The ID of the BigQuery dataset.
    table_id (str): The ID of the BigQuery table.

    Returns:
    None
    """
    df = pd.read_csv(csv_file_path, dtype=str)

    # Create dataset if doesn't exists
    dataset_ref = bq_client.dataset(dataset_id)
    try:
        bq_client.get_dataset(dataset_ref)
    except NotFound:
        print(f"Dataset {dataset_id} not found. Creating new dataset.")
        dataset = bigquery.Dataset(dataset_ref)
        dataset = bq_client.create_dataset(dataset)
        print(f"Dataset {dataset_id} created.")

    table_ref = dataset_ref.table(table_id)

    # Upload table
    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config).result()
    print(f"Data loaded into '{dataset_id}.{table_id}'")