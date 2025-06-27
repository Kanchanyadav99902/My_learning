import logging
from cryptography.fernet import Fernet
import psycopg2 
import os
from dotenv import load_dotenv
load_dotenv()
def encrypt_password():
    pass_key= Fernet.generate_key()
    cipher_suite = Fernet(pass_key)
    ciphered_text = cipher_suite.encrypt(b"R5sWDsMWc7aYHfQc")
    decrypted_pass =cipher_suite.decrypt(ciphered_text).decode()
    try:
        connection = psycopg2.connect(
                database  = os.getenv('DBNAME'),
                user = os.getenv('USER'),
                host = os.getenv('HOST'),
                # password = os.getenv('PASSWORD'),
                password = decrypted_pass,
                port = os.getenv('PORT')
                )
        print(connection)
        return connection
    except Exception as e:
        logging.error(e)
    # service_account_key = os.getenv('SERVICE_ACCOUNT_KEY')
    # print(f'printing the service account key {service_account_key}')
    # bg_client = bigquery.Client.from_service_account_json(service_account_key)
encrypt_password()