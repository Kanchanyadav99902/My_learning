# from google.cloud import storage
from google.cloud import storage, bigquery
import os
from dotenv import load_dotenv
# from google.cloud import storage_control_v2
import logging

load_dotenv()
bucket_name = os.getenv('BUCKET_NAME')
dataset_name_you_want  = os.getenv('DATASET_NAME_YOU_WANT')
project_name  = os.getenv('PROJECT_NAME')
location  = os.getenv('LOCATION')
tablename = os.getenv('TABLE_NAME')
folder= os.getenv('FOLDER_NAME')
service_account = os.getenv('SERVICE_ACCOUNT')
client = storage.Client.from_service_account_json(service_account)

buckets = list(client.list_buckets())
# for bucket in buckets:

#     if bucket.name == bucket_name:

#         blobs = bucket.list_blobs()
#         for blob in blobs:
#             print(blob.name)

class Automate:
    def __init__(self, buck_name,dataset_name_you_want, project_name, location,tablename,folder_name):
        self.bn = buck_name
        self.dataset= dataset_name_you_want
        self.project_name = project_name
        self.locn= location
        self.table = tablename
        self.foldname= folder_name

    def connection(self,path):
        storage_client = storage.Client.from_service_account_json(path)
        bg_client = bigquery.Client.from_service_account_json(path)
        return storage_client,bg_client
    
    def check_or_create_bucket(self,storage_client):
        if list(storage_client.list_buckets()) !=0:
            lst_of_files = []
            buckets = list(storage_client.list_buckets())
            for bucket in buckets:
                if bucket.name == self.bn:
                    files = list(bucket.list_blobs())
                    if not files:
                        raise Exception('Bucket is Empty')
                    # for file in files:
                    #     lst_of_files.append(file.name)
            return files
        else: # create a bucket
            bucket = storage_client.bucket(self.bn)
            bucket.storage_class = "STANDARD"
            new_bucket = storage_client.create_bucket(bucket, location=self.locn)
            return f'The bucket {new_bucket} is been created in {self.locn}'

    # def create_folder_inside_bucket(self):
    #     try:
    #         storage_control_client = storage_control_v2.StorageControlClient()
    #         """ 
    #             storage_control_v2: this is the library
    #             StorageControlClient(): create an object to make an api call
    #             storage_control_client: this variable stores our client object 
    #         """
    #             # The storage bucket path uses the global access pattern, in which the "_"
    #             # denotes this bucket exists in the global namespace.
    #         project_path = storage_control_client.common_project_path("_")
    #         bucket_path = f"{project_path}/buckets/{self.bn}"
    #         request = storage_control_v2.CreateFolderRequest(
    #                 parent=bucket_path,
    #                 folder_id=self.foldname,
    #             )
    #         response = storage_control_client.create_folder(request=request)
    #         print(response)
    #     except Exception as e:
    #         logging.error(f"An error occurred: {e}")

        

    def create_dataset(self, bg_client):
        # note: dataset_id = dataset name (Letters, numbers, and underscores allowed)
        dataset_full_name_in_bigquery = f'{self.project_name}.{self.dataset}' # this is dataset_id
        dataset_name = bigquery.Dataset(dataset_full_name_in_bigquery)
        dataset_name.location=self.locn
        final_created_ds = bg_client.create_dataset(dataset_name, timeout=30)  # Make an API request.
        return final_created_ds

    def delete_dataset(self, bg_client):
        ''' 
            The delete dataset 'delete_dataset' takes 3 parameters {dataset_id, delete_contents=True, not_found_ok=True}

        '''
        dataset_full_name_in_bigquery = f'{self.project_name}.{self.dataset}'
        print(dataset_full_name_in_bigquery)
        # dataset_name = bigquery.Dataset(dataset_full_name_in_bigquery) # this is dataset_id
        delete = bg_client.delete_dataset(dataset_full_name_in_bigquery,delete_contents=True, not_found_ok=True)
        
        return f'the dataset {dataset_full_name_in_bigquery} has been delete, {delete}'
    
    def create_table(self,bg_client,datasetname, tablename):
        table_id = f'{self.project_name}.{datasetname}.{tablename}'
        bg_client.create_table(table_id)
        return f'the table_name {table_id} is been created'

    def delete_table(self,bg_client):
        table_id = f'{self.project_name}.{self.dataset}.{self.table}'
        bg_client.delete_table(table_id, not_found_ok=True)
        return f'the table_name {table_id} is been deleted'


test= Automate(bucket_name,dataset_name_you_want, project_name, location, tablename,folder)
conn = test.connection(service_account)
# print(conn)
# fils = test.check_bucket(conn[0])
# print(fils)
# create_dataset = test.check_or_create_bucket(conn[1])
# delete_dataset = test.delete_dataset(conn[1])
# print(delete_dataset)
# print(test.delete_table(conn[1]))
# test.create_table(conn[1],'test_dataset','kanom')
# test.create_folder_inside_bucket()