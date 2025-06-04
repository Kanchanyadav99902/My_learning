from automate_gcp import Automate as a
import os
import pandas as pd
from google.cloud import bigquery

def cs_file_trigger(event,context):

    service_account = os.getenv('SERVICE_ACCOUNT')
    conn = a.connection(service_account)

    # df =  pd.read_csv()
    # create table 
    dataset_name = 'test_dataset'
    tablename = 'kanom'
    table = a.create_table(conn,dataset_name, tablename)
    uri = 'gs://ky_bkt/test_excel.xlsx'
    job = conn.load_table_from_uri(
        uri,
        "project.dataset._temp_table",
        job_config=bigquery.LoadJobConfig(
            autodetect=True,
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition="WRITE_TRUNCATE"
        )
    )

    job.result()
    schema = conn.get_table("project.dataset._temp_table").schema
    print(schema)

    # Optional: Delete temp table
    conn.delete_table("project.dataset._temp_table", not_found_ok=True)

cs_file_trigger()
