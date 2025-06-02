import functions_framework
import pandas as pd
from google.cloud import storage, bigquery
import tempfile
# from google.cloud import bigquery_storage_v1 

# cred= Client(project="AI Analytics", credentials=None, _http=None, 
    #    client_info=None, client_options=None, use_auth_w_custom_endpoint=True, extra_headers={}, *, api_key=None)

bucket_nm = 'ky_bkt'
file_name= 'test_excel.xlsx'

storage_client = storage.Client()
blobs_list = storage_client.list_blobs(bucket_or_name='ky_bkt')
bucket = storage_client.bucket(bucket_nm)
# print(bucket)
stats = storage.Blob(bucket=bucket, name=file_name).exists(storage_client)
# print(stats)
# print(sum(1 for _ in blobs_list))
blob = bucket_nm.blob('my-test-file.txt')
# print(blob)



# Register a CloudEvent function with the Functions Framework
@functions_framework.cloud_event
def my_cloudevent_function(cloud_event):

    bucket_name = cloud_event.data['ky_bkt']
    file_name = cloud_event.data['test_excel.xlsx']
    files_to_process= []
    if not file_name.endswith((".xls", ".xlsx")):
        print(f'the files {file_name} is been skipped')
    files_to_process.append(file_name)
    print(f'the files that are processing are {files_to_process}')
      
    storage_client = storage.Client()
    bq_client = bigquery.Client()

    # Download the Excel file
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    with tempfile.NamedTemporaryFile(suffix=".xlsx") as temp_file:
        blob.download_to_filename(temp_file.name)

        df = pd.read_excel(temp_file.name)

        # BigQuery target
        dataset_id = "test_dataset"
        table_id = "ky_excel_test"  
        table_ref = bq_client.dataset(dataset_id).table(table_id)

        # Load to BigQuery in append mode
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=True 
        )

        load_job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        load_job.result() 

        print(f"Appended {len(df)} rows to {dataset_id}.{table_id}")
