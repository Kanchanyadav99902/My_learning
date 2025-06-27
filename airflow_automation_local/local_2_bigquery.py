from automate_gcp import test
import automate_gcp as a
import os
import pandas as pd
from google.cloud import bigquery
import warnings
warnings.filterwarnings('ignore')
import logging
import random

service_account = os.getenv('SERVICE_ACCOUNT')
bg_client_conn = test.connection(service_account)[1]

dataset= 'test_dataset'
local_file = "C:\\Users\\Admin\\Downloads\\April 2025 logger.xlsx"
# local_file= r"C:\Users\Admin\Downloads\test_excel.xlsx"
table_name = str(local_file)[int(str(local_file).rfind("\\"))+1 :].replace(".xlsx",'')
table_id = f'{a.project_name}.{dataset}.{table_name}'


""" No need to create a table using api because it auto creates the table when we try to load the data"""

def transformation(local_file):
    df = pd.read_excel(local_file)
    cleaned_cols = list(df.columns.str.strip().str.lower())
    unique_col_set = set()
    seen_col_index = []
    for index, cols in enumerate(cleaned_cols):
        if cols not in unique_col_set:
            unique_col_set.add(cols)  # set is made to segregate the unique cols from the duplicates ones
            seen_col_index.append(index)  # unique indexs are added in the list 
    # removing unnecessary columns
    df = df.iloc[:, seen_col_index]
    df.columns = [cleaned_cols[i] for i in seen_col_index]

    # Add more columns 
    new_col_salary = []
    new_col_first_name = []
    new_col_last_name = []
    first_name = ('kanchan', 'Pavan', 'Om', 'shivani')
    last_names = ('Yadav', 'Gupta', 'ahir','teli')
    for _ in range(df.shape[0]):
        new_col_first_name.append(random.choice(first_name))
        new_col_last_name.append(random.choice(last_names))
        new_col_salary.append(random.randint(50000,1500000))
    df['new_col_first_name'] = new_col_first_name
    df['new_col_last_name']= new_col_last_name
    df['new_col_salary'] = new_col_salary
    df.to_csv('excel_2_csv.csv', index =False)

    return f"The new columns are added and unnecessary columns are deleted from the table "

try: 
    val =transformation(local_file)
    print(val)
    csv_path = r'C:\Users\Admin\Downloads\prac\excel_2_csv.csv'

    n_df = pd.read_csv(csv_path)
    print(n_df.head())
    column_names =list(n_df.columns)

    my_schema = [bigquery.SchemaField(name, "STRING") for name in column_names]
    job_config = bigquery.LoadJobConfig(
        schema = my_schema, 
        autodetect=False,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV
        # write_deposition = bigquery.write_disposition.WRITE_APPEND
    )
    job_config.write_disposition="WRITE_APPEND"
    # with open(csv_path, "rb") as source_file:
    #     job = bg_client_conn.load_table_from_file(source_file, table_id, job_config=job_config)
    job = bg_client_conn.load_table_from_dataframe(n_df, table_id, job_config=job_config)
    job.result()  # Waits for the job to complete.

    table = bg_client_conn.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )
except Exception as e:
    logging.error(e)