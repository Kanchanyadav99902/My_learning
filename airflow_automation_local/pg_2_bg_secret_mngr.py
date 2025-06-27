from airflow.models.dag import DAG
from datetime import datetime, timedelta
from airflow.decorators import task
import logging
import pandas as pd
import psycopg2
import json
from google.cloud import secretmanager
from google.oauth2 import service_account
import re
from google.cloud import bigquery
from airflow.exceptions import AirflowException
from airflow.models import Variable
import warnings
warnings.filterwarnings('ignore')

project_id = Variable.get('project_id')
secret_name= Variable.get('psql_creds_secret_name')
version = '1'
google_service_account = '/opt/airflow/service_account.json' 
project_name = Variable.get('1f_project_name')
dataset = Variable.get('Dataset')
table_name= Variable.get('table_name')
table_id = f'{project_name}.{dataset}.{table_name}'
pg_table = 'amcs'

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id="postgres_2_bigquery_secret_mngr", 
        description= 'this dag is for postgres_2_bigquery_secret_mngr through airflow',
        schedule=None, 
        start_date=datetime(2022, 3, 4),
        catchup= False,
        default_args= default_args,
        tags = ['Data Engineer', 'Kanchan']
) as dag:
    @task(task_id = 'access_secrets_from_gcp')
    def access_secret_version():
 
        try:
            credentials = service_account.Credentials.from_service_account_file(google_service_account)
            client = secretmanager.SecretManagerServiceClient(credentials=credentials)
            secret_name_ = f"projects/{project_id}/secrets/{secret_name}/versions/{version}"
            response = client.access_secret_version(name=secret_name_)
            payload = response.payload.data.decode('UTF-8') 
            payload_cleaned = re.sub(r'"\s*"', '", "', payload)
            secrets = json.loads(payload_cleaned)
            print(type(secrets))
            print("Parsed secrets:", secrets)
            print(secrets['username'])
            return secrets
        
        except Exception as e:
            logging.error(e)
            raise AirflowException(e)

    @task(task_id = 'fetch_data_from_postgres')
    def pg_data_fetch(payload):

        secret= payload
        print(f'this is a secret')
        print(type(secret))  # this is to verify the the data which we are getting is in the form of dictionary

        try: 
            connection = psycopg2.connect(
                    database  = 'LakeMaster',
                    user = secret['username'],
                    host = secret['host'],
                    password = secret['password'],
                    port = secret['port']
                    )
            print(connection)
        except Exception as e:
            logging.error(e)
            raise AirflowException(e)
        try: 
            if not connection:
                print('the connection is not there, that means it is not able to connect to postgres')
        except Exception as e:
            logging.error(e)
        else:
            try:
                query = f'select * from {pg_table}'
                df = pd.read_sql_query(query,connection)
                print(df.head())
                df = df.astype(str)    #making all the columns to a string to easy load
                print(f'the data is being read the converted to a dataframe')
                return df
            except Exception as e:
                logging.error(e)
                raise Exception()
    
    @task(task_id = 'load_2_bigquery')
    def load_2_bigquery(df):

        try:
            bg_client = bigquery.Client.from_service_account_json(google_service_account)
            job_config = bigquery.LoadJobConfig(
                autodetect=True,
                source_format=bigquery.SourceFormat.CSV
            )
            job_config.write_disposition="WRITE_TRUNCATE"
            job = bg_client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()

            """ 
            after the data is being loaded we are getting the table information to print the number of records loaded
            """
            table = bg_client.get_table(table_id)  # Make an API request.
            print(
                "Loaded {} rows and {} columns to {}".format(
                    table.num_rows, len(table.schema), table_id
                )
            )
            return 'ETL successful'
        except Exception as e:
            logging.error(e)
            raise AirflowException(e)
    k  = access_secret_version()

    pg_df = pg_data_fetch(k)
    load_2_bg = load_2_bigquery(pg_df)
    
    k >> pg_df >> load_2_bg