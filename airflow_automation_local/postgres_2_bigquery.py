from airflow.models.dag import DAG 
from datetime import datetime, timedelta
import os
import textwrap
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import logging
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import task
import pandas as pd
import psycopg2
# from service_account import *
# from config.postgres_conn import encrypt_password
# import config.postgres_conn
# from postgres_conn import pg_conn
from config.postgres_conn import pg_conn
# from ..config.postgres_conn import pg_conn
from google.cloud import bigquery
from cryptography.fernet import Fernet
from dotenv import load_dotenv
load_dotenv()
# from postgres_conn import encrypt_password
# val = pg_conn()


# def encrypt_password():
#     pass_key= Fernet.generate_key()
#     cipher_suite = Fernet(pass_key)
#     ciphered_text = cipher_suite.encrypt(b"J'k%S$uf4M;y3#")
#     decrypted_pass =cipher_suite.decrypt(ciphered_text).decode()
#     try:
#         connection = psycopg2.connect(
#                 database  = os.getenv('DBNAME'),
#                 user = os.getenv('USER'),
#                 host = os.getenv('HOST'),
#                 # password = os.getenv('PASSWORD'),
#                 password = decrypted_pass,
#                 port = os.getenv('PORT')
#                 )
#         print(connection)
#         return connection
#     except Exception as e:
#         logging.error(e)

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(dag_id="example_task_mapping", 
        description= 'this dag is for locally testing the etf through airflow',
        schedule=None, 
        start_date=datetime(2022, 3, 4),
        catchup= False,
        default_args= default_args,
        tags = ['Data Engineer', 'Kanchan']
) as dag:
    # @task(task_id = 'pg_connection')
    # def pg_connection():
    #     pg_connectn = pg_conn()
    #     return pg_connectn

    @task(task_id='print_greeting_ky')
    def hello_kan():
        print("hello kanchan yadav")
        print("hello kanchan yadav ")

    @task(task_id= 'animal')
    def animal():
        print('this is a cat')
        k = pg_conn()
        print(k)

    
    @task(task_id = 'data_from_pgadmin')
    def pg_data_fetch():
        connection = pg_conn()
        print(connection)
        try: 
            if not connection:
                print('the connection is not there, that means it is not able to connect to postgres')
        except Exception as e:
            logging.error(e)
        else:
            schema = os.getenv('SCHEMA_NAME')
            table = os.getenv('TABLE_NAME')
            # query= 'select * from public.app_version_list'
            query = f'select * from {schema}.{table}'
            df = pd.read_sql_query(query,connection)
            # print(f'the data is being read the converted to a dataframe')
            print(df.head())

            print(f'the data is being read the converted to a dataframe')
            return df.to_json(orient="records")
        
    # # greet() >> pg_data_fetch()
    @task(task_id = 'transformation')
    def transform(df_json):
        df= pd.read_json(df_json)
        df['new_col'] = [row_ind for row_ind in range(df.shape[0])]
        df = df.drop(['scheme_code'], axis=1)
        print(df.tail(2))
        # df.to_json()
        return df.to_json()


    # # k = pg_data_fetch()
    # # transform(k)
    @task(task_id = 'load_2_bigquery')
    def load_2_bigquery(df_json_transform):
        df = pd.read_json(df_json_transform)  # or json.loads if using JSON
        print(df.head()) 
        df = df.fillna('kanchan')
        # service_account_key = {
        #     "type": "service_account",
        #     "project_id": "analytics-1f",
        #     "private_key_id": "607f3e39b749211be7b95ba98ad3cfc599e0e170",
        #     "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDR5ebgnnWg9kH6\nPt8yCt2ESM7xCv74f5Ux9Z/UaEK1EaarmItmhCHiO/vkOILYWjegSsNs/pWnFWg4\n2MwFfUxVRviqmA7l/rpBU6g1DF2HhO29kb4TPqSpbBrKaKg1SGJSrOwu+3B+1mqi\naxGrF5qscj5Ku9jNovOuPN5pI7+CyuqplpfpOe8He71whOpRSfprc1K+7nqa5CZV\n7j1edvU4K2hebEXB9Cokz/cIoh47Ib/se4cG/aYhMpzHxH6Hs8dkhro6zA32MQLE\nv0Tmvjg1kY3JkJfsJEoNbZqfh2tjWQ4pQCmtX3DVezO75iGwXvxRsXNfM0ZRhSTV\neCWWerLVAgMBAAECggEAAp3vhz/Pjb56LewoV6zOq9C3dgPa6jYfhZK7yqWD1S1R\nzydBYa2yUQuHtMOODQkNI7d0/ByMY6nr3dLoqQVhGvsBc3UelKex/MBC6TpoJYZO\njJRrZIi8UN0bfv6oMcemRZT6wRXxVum4Aquv66//0Xqzh32PJbRO04BIJfjNqi1Y\nrpgdayZ0SWzzelp5ID6XVhEuzvjRJ2094eArv+VMdQzYRCmYGmmd1Wx9fWJRr7XN\nninVia6X7VS6s/L6/t7VmB6k/O+tK5gM+JOzzNLxBlIgBUel5leDmK2d62zSFvnG\nsCQU0Pu225XRo4cKqXCoMYkdFVeBIR3p2EK9605MBwKBgQD9qY/mDCj/T39FWPsP\ngQCg0obzOuk+Iq8MsHVilB9vCwZ4qYKeK/zGGlU/qeigAc4RZX7n0CduAPEQx5LY\nFHdntbTuntbEfiUUmjpVRUo44Wz070seSnvfgivj4UYG4Omi2/P42gnxgtqahKnA\nM50O2s3ND9Ao9D+ajx/hVsX44wKBgQDT1Rdn/zW/EnOIS1Sd2Rhy7+l4cdUlF68P\nKoMG4EURV9nJ8H8GL0i1mRyqH3e3pIEGmBmpWdnjAASLyErn8uRFOo37qi9skTu6\n0cDCDEpJvmNTSy1M0f302tDmGmKL0tcZ5D5+e2JPvWdNtfJIWmicZyIvwVJilB2j\n9S1c3PzK5wKBgDL8gy0VYqQ24XkZ8pTiNDMfPfrj64AOzqq5SU8AQPU7/RgB0nxi\nkRNstZfvWg/XbQk4InML9vD4hXv+8Xg3XOzerDiOetLSHDfw7Cq5m7qlLRZqhunW\nxOFfM91BKac2TB/tIDPz962wxKnsYoZ8mWQj/NvsAsZImgRCIoFTxMVLAoGAHPA6\nb1mdBQpHQDJST/INdf4yXeLnJFcU8jRzi+ftZ44h0YWKRNUG0NAm61K7LxpJChaT\nQuBkdK9W76WgHgm/h3mVVfEM437/sujetWOqbmKU/RcDDECPufh+m0C1f7aA7ieR\nJgRDDPOp/iLCA4JeUieAsxds4syrM+/Vj58c5bcCgYEA+jZT+YVhYkVGPda5WBbA\nRbCIqk7fHuXwLW1WJx3xXT5XkhZMkrnjvhrGwzHJDo81Mp4yk3L8K6WW6y63fZBL\nUzgNE07VY5azbJfYs2FXkX2KG4jV5/noxKBIF5E/uodUxZTJay1AkHX4CkMyD6X7\nyqMvBNY3EWogjZoR3KwZBp4=\n-----END PRIVATE KEY-----\n",
        #     "client_email": "analytics-pipeline@analytics-1f.iam.gserviceaccount.com",
        #     "client_id": "101087825995853507328",
        #     "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        #     "token_uri": "https://oauth2.googleapis.com/token",
        #     "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        #     "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/analytics-pipeline%40analytics-1f.iam.gserviceaccount.com"
        #     }
        # service_account_key = os.getenv('SERVICE_ACCOUNT_KEY')
        service_account_key = "/opt/airflow/service_account.json"
        print(f'printing the service account key {service_account_key}')
        bg_client = bigquery.Client.from_service_account_json(service_account_key)

        columns  = list(df.columns)
        # my_schema = [bigquery.SchemaField(name, "STRING") for name in columns]
        # print(my_schema)
        dataset = 'Test_DB'
        project_name = os.getenv('PROJECT_NAME').lower()
        def get_bq_type(dtype):
            if pd.api.types.is_integer_dtype(dtype):
                return "INTEGER"
            elif pd.api.types.is_float_dtype(dtype):
                return "FLOAT"
            elif pd.api.types.is_bool_dtype(dtype):
                return "BOOLEAN"
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                return "TIMESTAMP"
            else:
                return "STRING"

        my_schema = [
            bigquery.SchemaField(name, get_bq_type(dtype))
            for name, dtype in df.dtypes.items()
        ]

        # client = bigquery.Client()
        # lower_project_id = lo
        table_id = f'{project_name}.{dataset}.mf_overlap_funds'
        job_config = bigquery.LoadJobConfig(
            schema = my_schema, 
            autodetect=True,
            # skip_leading_rows=1
            )
        job_config.write_disposition="WRITE_APPEND"

        job = bg_client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result() 
        table = bg_client.get_table(table_id)
        print(
        "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
    # postgres_conn = pg_conn()
    # print(pg_conn)
    # kanchan = pg_connection()
    hello_task = hello_kan()
    animal_task = animal()
    postgres_data_fetch = pg_data_fetch()
    postgres_data_clean = transform(postgres_data_fetch)
    load_2_bigquer= load_2_bigquery(postgres_data_clean)

    # Optionally, define task order:
    hello_task >> animal_task  >> postgres_data_fetch >> postgres_data_clean >> load_2_bigquer

  
    # data = pg_data_fetch() 
    # greet() >>  [load_2_bigquery(transform(data))]