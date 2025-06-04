import pypyodbc 
import pandas as pd
import pyodbc
from datetime import date, timedelta
from google.cloud import bigquery
import pyarrow as pa
# drivers = pyodbc.drivers()
# print("Installed ODBC Drivers:")
# for driver in drivers:
#     print(driver)
import os

service_account = os.getenv('SERVICE_ACCOUNT')
bg_client = bigquery.Client.from_service_account_json(service_account)

def ssms_connection():
    conn_str = (
        r'Driver={ODBC Driver 18 for SQL Server};'  # installed driver
        r'Server=Rahul-Work\SQLEXPRESS;'                  # e.g. 'localhost\SQLEXPRESS'
        r'Database=kydb;'
        r'Trusted_Connection=yes;' 
        r'TrustServerCertificate=yes;'
                                            
    )


    conn = pyodbc.connect(conn_str)
    print(f'connection established {conn}')
    # cursor = conn.cursor()

    # query = "select * from dbo.kan_user"
    # cursor.execute(query)

    # # cursor.execute("SELECT TOP 10 * FROM YourTableName")
    # rows = cursor.fetchall()
    # print(rows)
    return conn

def create_a_table(conn):
    cursor = conn.cursor()
    create_table_sql = """
        IF OBJECT_ID('Employees', 'U') IS NULL
        BEGIN
            CREATE TABLE Employees (
                EmployeeID INT PRIMARY KEY,
                FirstName VARCHAR(50),
                LastName VARCHAR(50),
                Email VARCHAR(100),
                HireDate DATE
            )
        END
        """

    cursor.execute(create_table_sql)
    # conn.commit()

    first_names = ['John', 'Jane', 'Michael', 'Emily', 'David', 'Laura', 'Robert', 'Linda', 'James', 'Susan']
    last_names = ['Smith', 'Johnson', 'Williams', 'Jones', 'Brown', 'Davis', 'Miller', 'Wilson', 'Moore', 'Taylor']

    start_date = date(2020, 1, 1)

    for i in range(1, 51):
        fname = first_names[i % len(first_names)]
        lname = last_names[i % len(last_names)]
        email = f"{fname.lower()}.{lname.lower()}{i}@example.com"
        hire_date = start_date + timedelta(days=i*10)
        
        cursor.execute("""
            INSERT INTO Employees (EmployeeID, FirstName, LastName, Email, HireDate)
            VALUES (?, ?, ?, ?, ?)
        """, (i, fname, lname, email, hire_date))

    conn.commit()
    print("Inserted 50 sample records into Employees table.")
k= ssms_connection()
# create_a_table(k)

def load_2_bigquery(conn):
    cursor = conn.cursor()
    columns = """ select column_name from INFORMATION_SCHEMA.COLUMNS
    where TABLE_SCHEMA = 'dbo' and TABLE_NAME= 'employees'
    """
    cursor.execute(columns)
    columns = cursor.fetchall()
    colum = [col[0] for col in columns]
    print(f'the length of columsn are {len(colum)}')
    data = 'SELECT * FROM [dbo].Employees ' 
    df = pd.read_sql(data, conn)
    # df =pd.DataFrame(rows, columns= colum)
    print(df.head())

    # my_schema = [bigquery.SchemaField(name, "STRING") for name in colum]
    job_config = bigquery.LoadJobConfig(
        # schema = my_schema, 
        autodetect=True
        # skip_leading_rows=1
    )
    job_config.write_disposition="WRITE_APPEND"
    # dataset = os.getenv('DATASET_NAME_YOU_WANT')
    dataset = 'test_dataset'
    project_name = os.getenv('PROJECT_NAME')
    # table_name = str(local_file)[int(str(local_file).rfind("\\"))+1 :].replace(".xlsx",'')
    table_id = f'{project_name}.{dataset}.employees'
    job = bg_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Waits for the job to complete.

    table = bg_client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )


load_2_bigquery(k)