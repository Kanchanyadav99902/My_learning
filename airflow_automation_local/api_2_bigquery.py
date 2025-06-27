# import requests
# import pandas as pd
# import requests
# import os
# from google.cloud import bigquery
# url = 'https://onehub.1finance.co.in/scoring-ranking/pm-well/get-logs'
# headers = {
#     'Authorization': 'Bearer Z3bXPL+I1dfVflX45XjtXCrU0SsOTrzbKca0YAiSJp0='
# }
# project_id = 'analytics-1f'
# dataset_id = 'pm_well'
# table_name = 'scoring_ranking_prod'
# table_id = f'{project_id}.{dataset_id}.{table_name}'
# response = requests.get(url, headers=headers)

# # Check for success
# if response.status_code == 200:
#     data = response.json()  # if response is in JSON
#     # print(data['Logs'])  # or process the data
# else:
#     print(f"Request failed with status: {response.status_code}")
#     print(response.text)

# df = pd.DataFrame(data['Logs'])
# print(df.dtypes)

# # df.created_at = df.created_at.dt.strftime('%Y-%m-%d')
# df['created_at'] = pd.to_datetime(df['created_at'])
# df['updated_at'] = pd.to_datetime(df['updated_at'])
# df['Created Date'] = df['created_at'].dt.strftime('%Y-%m-%d')
# df['Created Time'] = df['created_at'].dt.strftime('%H:%M:%S')
# columns_k = {'Slug': 'Url',
#            'AgeGroup': 'Age Group',
#            'SumInsured': 'Sum Insured',
#            'FamilyConstructMapping':'Family Construct Mapping',
#            'UserIP': 'User IP'}
# df.rename(columns=columns_k, inplace=True)
# print(df.shape)
# print(df.head())


# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service_account.json"
# client = bigquery.Client()
# print(client)
# column_names = list(df.columns)
# print(df.columns)
# # df.to_csv('excel_2_csv.csv', index =False)
# my_schema = [bigquery.SchemaField(name, "STRING") for name in column_names]
# job_config = bigquery.LoadJobConfig(
#     schema = my_schema, 
#     autodetect=False,
#     # skip_leading_rows=1,
#     source_format=bigquery.SourceFormat.CSV
#     # write_deposition = bigquery.write_disposition.WRITE_APPEND
# )


# job_config.write_disposition="WRITE_APPEND"
# job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
# job.result()  # Waits for the job to complete.

# table = client.get_table(table_id)  # Make an API request.
# print(
#     "Loaded {} rows and {} columns to {}".format(
#         table.num_rows, len(table.schema), table_id
#     )
# )

# import functions_framework
# import requests
# import pandas as pd
# import requests
# import os
# from google.cloud import bigquery

# @functions_framework.http
# def main(request):

#     url = 'https://onehub.1finance.co.in/scoring-ranking/pm-well/get-logs'
#     headers = {
#         'Authorization': 'Bearer Z3bXPL+I1dfVflX45XjtXCrU0SsOTrzbKca0YAiSJp0='
#     }
        
#     response = requests.get(url, headers=headers)

#     # Check for success
#     if response.status_code == 200:
#         data = response.json()  # if response is in JSON
#         df = pd.DataFrame(data['Logs'])
#         # print(data['Logs'])  # or process the data
#     else:
#         print(f"Request failed with status: {response.status_code}")
#         print(response.text)

#     # df = pd.DataFrame(data['Logs'])
#     print(df.dtypes)

#     # df.created_at = df.created_at.dt.strftime('%Y-%m-%d')
#     df['created_at'] = pd.to_datetime(df['created_at'])
#     df['updated_at'] = pd.to_datetime(df['updated_at'])
#     df['Created Date'] = df['created_at'].dt.strftime('%Y-%m-%d')
#     df['Created Time'] = df['created_at'].dt.strftime('%H:%M:%S')
#     columns_k = {'Slug': 'Url',
#             'AgeGroup': 'Age Group',
#             'SumInsured': 'Sum Insured',
#             'FamilyConstructMapping':'Family Construct Mapping',
#             'UserIP': 'User IP'}
#     df.rename(columns=columns_k, inplace=True)
#     print(df.shape)
#     print(df.head())

#     column_names = list(df.columns)
#     print(df.columns)
#     client = bigquery.Client()
#     # df.to_csv('excel_2_csv.csv', index =False)
#     my_schema = [bigquery.SchemaField(name, "STRING") for name in column_names]
#     job_config = bigquery.LoadJobConfig(
#         schema = my_schema, 
#         autodetect=False,
#         # skip_leading_rows=1,
#         source_format=bigquery.SourceFormat.CSV
#         # write_deposition = bigquery.write_disposition.WRITE_APPEND
#     )


#     job_config.write_disposition="WRITE_APPEND"
#     job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
#     job.result()  # Waits for the job to complete.

#     table = client.get_table(table_id)  # Make an API request.
#     print(
#         "Loaded {} rows and {} columns to {}".format(
#             table.num_rows, len(table.schema), table_id
#         )
#     )

# import functions_framework
import requests
import pandas as pd
import requests
import os
from google.cloud import bigquery

def send_notification(message):
    chat_webhook_url = 'https://chat.googleapis.com/v1/spaces/AAAAuQ0M9Ko/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=1S5_gZwX1zrjjhDkGaHR-2KcJUhuIOP3E48uHTqtV-k'
    
    headers = {'Content-Type': 'application/json; charset=UTF-8'}
    data = {
        'text': message
    }
    response = requests.post(chat_webhook_url, headers=headers, data=json.dumps(data))
    if response.status_code != 200:
        print(f"Failed to send notification. Status code: {response.status_code}")
    else:
        print("Notification sent successfully.")

# @functions_framework.http
def main():
    try:
        url = 'https://onehub.1finance.co.in/scoring-ranking/pm-well/get-logs'
        headers = {
            'Authorization': 'Bearer Z3bXPL+I1dfVflX45XjtXCrU0SsOTrzbKca0YAiSJp0='
        }
        project_id = 'analytics-1f'
        dataset_id = 'pm_well'
        table_name = 'scoring_ranking_prod'
        table_id = f'{project_id}.{dataset_id}.{table_name}'
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Admin\Downloads\airflow_automation\service_account.json"
        client = bigquery.Client()
        # print(client)
        try:
            response = requests.get(url, headers=headers)

            # Check for success
            if response.status_code == 200:
                data = response.json()  # if response is in JSON
                df = pd.DataFrame(data['Logs'])
                # print(data['Logs'])  # or process the data
            else:
                print(f"Request failed with status: {response.status_code}")
                print(response.text)
        except Exception as e:
            msg = f'pipeline failed with exception while fetching data from the api:-  {e}'
            # send_notification(msg)
            return 'ETL pipeline failed while fetching the data from api'

        # df = pd.DataFrame(data['Logs'])
        print(df.dtypes)

        # df.created_at = df.created_at.dt.strftime('%Y-%m-%d')
        df['created_at'] = pd.to_datetime(df['created_at'])
        df['updated_at'] = pd.to_datetime(df['updated_at'])
        df['Created_Date'] = df['created_at'].dt.strftime('%Y-%m-%d')
        df['Created_Time'] = df['created_at'].dt.strftime('%H:%M:%S')
        columns_k = {'Slug': 'Url',
                'AgeGroup': 'Age_Group',
                'SumInsured': 'Sum_Insured',
                'FamilyConstructMapping':'Family_Construct_Mapping',
                'UserIP': 'User_IP'}
        df.rename(columns=columns_k, inplace=True)
        print(df.shape)
        print(df.head())

        column_names = list(df.columns)
        print(df.columns)
        client = bigquery.Client()
        # df.to_csv('excel_2_csv.csv', index =False)
        my_schema = [bigquery.SchemaField(name, "STRING") for name in column_names]
        job_config = bigquery.LoadJobConfig(
            schema = my_schema, 
            autodetect=False,
            # skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV
            # write_deposition = bigquery.write_disposition.WRITE_APPEND
        )


        job_config.write_disposition="WRITE_TRUNCATE"
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Waits for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
        return 'ETL successful'
    except Exception as e:
        message = f'pipeline failed with exception {e}'
        print(message)
        # send_notification(message)
        return 'ETL pipeline failed'
    
main()