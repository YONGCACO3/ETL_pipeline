# To instantiate a DAG
from airflow import DAG
# Operators
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import timedelta
# To unzip the dataset
import tarfile
import pandas as pd
# To convert the timestamp
import pendulum
# To load data to PostgreSQL
from sqlalchemy import create_engine
# Using Kafka for data streaming
from confluent_kafka import Producer, Consumer

# Define DAG arguments
default_args = {
    'owner': 'Jiayong',
    'start_date': pendulum.today('UTC').add(days=-0),  # start today
    'email': 'dummy@mail.com',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Build an ETL Pipeline using Airflow',
    schedule=timedelta(days=1),  # run the DAG daily
)

# Define the BashOperator task for downloading the dataset
download_dataset_task = BashOperator(
    task_id='download_dataset',
    # use curl to download the dataset to the path
    bash_command='curl -o /Users/jiayonglu/airflow/dags/finalassignment/tolldata.tgz https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz',
    dag=dag,
)

# Define the Python function for the unzip_data task


def unzip_data_function(**kwargs):
    with tarfile.open("/Users/jiayonglu/airflow/dags/finalassignment/tolldata.tgz", "r:gz") as tar:
        tar.extractall("/Users/jiayonglu/airflow/dags/finalassignment")


unzip_data_task = PythonOperator(
    task_id='unzip_data',
    python_callable=unzip_data_function,
    dag=dag,
)

# Define the Python function for the extract_data_from_csv task


def extract_data_from_csv_function(**kwargs):
    df = pd.read_csv(
        "/Users/jiayonglu/airflow/dags/finalassignment/vehicle-data.csv")

    # the original columns are unnamed, let's add column names
    new_df = df.iloc[:, :4].copy()  # Copy the selected part to a new DataFrame
    new_df.columns = ["Rowid", "Timestamp",
                      "Anonymized Vehicle number", "Vehicle type"]

    new_df.to_csv(
        "/Users/jiayonglu/airflow/dags/finalassignment/csv_data.csv", index=False)


extract_data_from_csv_task = PythonOperator(
    task_id='extract_data_from_csv',
    python_callable=extract_data_from_csv_function,
    dag=dag,
)

# Define the Python function for the extract_data_from_tsv task


def extract_data_from_tsv_function(**kwargs):
    df = pd.read_csv(
        "/Users/jiayonglu/airflow/dags/finalassignment/tollplaza-data.tsv", sep='\t')
    # Copy the selected part to a new DataFrame
    new_df = df.iloc[:, 4:7].copy()
    new_df.columns = ["Number of axles", "TTollplaza id", "Tollplaza code"]
    new_df.to_csv(
        "/Users/jiayonglu/airflow/dags/finalassignment/tsv_data.csv", index=False)


extract_data_from_tsv_task = PythonOperator(
    task_id='extract_data_from_tsv',
    python_callable=extract_data_from_tsv_function,
    dag=dag,
)

# Define the Python function for the extract_data_from_fixed_width task


def extract_data_from_fixed_width_function(**kwargs):
    # Read fixed-width data into a DataFrame
    df = pd.read_fwf("/Users/jiayonglu/airflow/dags/finalassignment/payment-data.txt",
                     colspecs=[(58, 61), (62, 66)],
                     header=None, names=['Type of Payment code', 'Vehicle Code'])

    # Write the DataFrame to a CSV file
    df.to_csv(
        "/Users/jiayonglu/airflow/dags/finalassignment/fixed_width_data.csv", index=False)


extract_data_from_fixed_width_task = PythonOperator(
    task_id='extract_data_from_fixed_width',
    python_callable=extract_data_from_fixed_width_function,
    dag=dag,
)

# Define the Python function for the consolidate_data task


def consolidate_data_function(**kwargs):
    df_csv = pd.read_csv(
        "/Users/jiayonglu/airflow/dags/finalassignment/csv_data.csv")
    df_tsv = pd.read_csv(
        "/Users/jiayonglu/airflow/dags/finalassignment/tsv_data.csv")
    df_fixed_width = pd.read_csv(
        "/Users/jiayonglu/airflow/dags/finalassignment/fixed_width_data.csv")

    consolidated_df = pd.concat([df_csv, df_tsv, df_fixed_width], axis=1)
    consolidated_df.to_csv(
        "/Users/jiayonglu/airflow/dags/finalassignment/extracted_data.csv", index=False)


consolidate_data_task = PythonOperator(
    task_id='consolidate_data',
    python_callable=consolidate_data_function,
    dag=dag,
)

# Define the Python function for the transform_data task


def transform_data_function(**kwargs):
    df = pd.read_csv(
        "/Users/jiayonglu/airflow/dags/finalassignment/extracted_data.csv")
    df.iloc[:, 3] = df.iloc[:, 3].str.upper()
    df.to_csv(
        "/Users/jiayonglu/airflow/dags/finalassignment/transformed_data.csv", index=False)


transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data_function,
    dag=dag,
)


# Define the Python function for loading data to PostgreSQL
def load_data_to_postgres():
    try:
        # Establish connections
        # Replace with your own PostgreSQL credentials
        conn_string = 'postgresql://username:password@host:port/database'
        db = create_engine(conn_string)
        conn = db.connect()

        # Drop table if it already exists
        conn.execute('DROP TABLE IF EXISTS toll_data')

        # Create the toll_data table
        sql = '''
        CREATE TABLE toll_data (
            Rowid INT,
            "Timestamp" TIMESTAMP,
            Anonymized_Vehicle_number INT,
            Vehicle_type VARCHAR(10),
            Number_of_axles INT,
            Tollplaza_id INT,
            Tollplaza_code VARCHAR(10),
            Type_of_Payment_code VARCHAR(5),
            Vehicle_Code VARCHAR(5)
        );
        '''
        conn.execute(sql)

        # Import the CSV file to create a DataFrame
        data = pd.read_csv(
            "/Users/jiayonglu/airflow/dags/finalassignment/transformed_data.csv")

        # Convert data to SQL and load into PostgreSQL
        data.to_sql('toll_data', conn, if_exists='replace', index=False)

        conn.close()

        print("Data loaded successfully.")
    except Exception as e:
        print(f"Error: {e}")
        raise e


# Define the PythonOperator task
load_data_task = PythonOperator(
    task_id='load_data_to_postgres',
    python_callable=load_data_to_postgres,
    dag=dag,
)

# Set task dependencies
download_dataset_task >> unzip_data_task >> [extract_data_from_csv_task, extract_data_from_tsv_task,
                                             extract_data_from_fixed_width_task] >> consolidate_data_task >> transform_data_task >> load_data_task


# #integrate kafka
# def load_data_to_kafka(**kwargs):
#     try:
#         # Establish connections
#         conn_string = 'postgresql://username:password@host:port/database' # Replace with your own PostgreSQL credentials
#         db = create_engine(conn_string)
#         conn = db.connect()

#         # Import the CSV file to create a DataFrame
#         data = pd.read_csv(
#             "/Users/jiayonglu/airflow/dags/finalassignment/transformed_data.csv")

#         # Convert data to JSON and produce to Kafka topic
#         producer_conf = {
#             'bootstrap.servers': 'your_kafka_broker',  # Replace with your Kafka broker address
#             'client.id': 'airflow-producer'
#         }
#         producer = Producer(producer_conf)

#         for _, row in data.iterrows():
#             # Convert each row to a JSON message
#             message = row.to_json()

#             # Produce the message to the Kafka topic
#             producer.produce('your_kafka_topic', key=str(row['Rowid']), value=message)

#         producer.flush()
#         conn.close()

#         print("Data loaded to Kafka successfully.")
#     except Exception as e:
#         print(f"Error: {e}")
#         raise e

# # Define the PythonOperator task for loading data to Kafka
# load_data_to_kafka_task = PythonOperator(
#     task_id='load_data_to_kafka',
#     python_callable=load_data_to_kafka,
#     dag=dag,
# )


# Define tasks using BashOperator
# # Define the unzip_data task
# unzip_data_task = BashOperator(
#     task_id='unzip_data',
#     # define the path of the tar file and the path to extract the files
#     bash_command='sudo tar -xzf /Users/jiayonglu/airflow/dags/finalassignment/tolldata.tgz -C /Users/jiayonglu/airflow/dags/finalassignment',
#     dag=dag,
# )

# # Define the extract_data_from_csv task
# extract_data_from_csv_task = BashOperator(
#     task_id='extract_data_from_csv',
#     # use the cut command to extract the first 4 columns of the csv file
#     bash_command='cat /Users/jiayonglu/airflow/dags/finalassignment/vehicle-data.csv | cut -d"," -f1,2,3,4 > /Users/jiayonglu/airflow/dags/finalassignment/csv_data.csv',
#     dag=dag,
# )

# # Define the extract_data_from_tsv task
# extract_data_from_tsv_task = BashOperator(
#     task_id='extract_data_from_tsv',
#     bash_command='cat /Users/jiayonglu/airflow/dags/finalassignment/tollplaza-data.tsv | cut -f5,6,7 > /Users/jiayonglu/airflow/dags/finalassignment/tsv_data.csv',
#     dag=dag,
# )

# # Define the extract_data_from_fixed_width task
# extract_data_from_fixed_width_task = BashOperator(
#     task_id='extract_data_from_fixed_width',
#     # use the awk command to extract the last two fields (columns) of the dataset
#     bash_command='awk "{print $(NF-1), $NF}" /Users/jiayonglu/airflow/dags/finalassignment/payment-data.txt > /Users/jiayonglu/airflow/dags/finalassignment/fixed_width_data.csv',
#     dag=dag,
# )


# # Define the consolidate_data task
# consolidate_data_task = BashOperator(
#     task_id='consolidate_data',
#     bash_command='paste /Users/jiayonglu/airflow/dags/finalassignment/csv_data.csv /Users/jiayonglu/airflow/dags/finalassignment/tsv_data.csv /Users/jiayonglu/airflow/dags/finalassignment/fixed_width_data.csv > /Users/jiayonglu/airflow/dags/finalassignment/extracted_data.csv',
#     dag=dag,
# )

# # Define the transform_data task
# transform_data_task = BashOperator(
#     task_id='transform_data',
#     bash_command='awk \'BEGIN {OFS=FS=","} { $4=toupper($4); print }\' /Users/jiayonglu/airflow/dags/finalassignment/extracted_data.csv > /Users/jiayonglu/airflow/dags/finalassignment/staging/transformed_data.csv',
#     dag=dag,
# )
