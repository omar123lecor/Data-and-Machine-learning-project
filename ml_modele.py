import os

import s3fs
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta,datetime
from io import StringIO
import boto3
import tempfile
from io import BytesIO
import pandas as pd
import pickle
from catboost import CatBoostRegressor
from datetime import datetime
import time
def load_from_s3(**kwargs):
    aws_access_key_id = 'Your_id_key'
    aws_secret_access_key = 'the_secret_one'
    # Initialize the S3 client
    current_time = time.localtime()
    current_datetime = time.strftime("%Y-%m", current_time)
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    # S3 bucket details
    bucket_name = 'Your_bucket_name'
    s3_key = f'valorant{current_datetime}.csv'  # The key of the file you uploaded

    # Get the object from S3
    response = s3.get_object(Bucket=bucket_name, Key=s3_key)

    # Read the content of the file
    file_content = response['Body'].read().decode('utf-8')

    # Optional: Convert the content to a pandas DataFrame if it's a CSV
    csv_data = StringIO(file_content)
    df = pd.read_csv(csv_data)
    df = df.drop(df.columns[0],axis=1)
    #df.to_csv('what.csv')
    return df

def train_model(**kwargs):
    #Extraction du fichier
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='load_from_s3')  # Récupère les données depuis XCom
    Df = df
    X = Df.drop(columns=['Ratio K/D'])
    y = Df['Ratio K/D']
    categorical_features = ['Rank','Map','Agent']
    model = CatBoostRegressor(
        iterations=500,
        learning_rate=0.01,
        depth=10,
        min_child_samples=17,
        cat_features=categorical_features,
        verbose=False
    )
    model.fit(X,y,verbose=True)
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        model.save_model(tmp_file.name,format='cbm')
        tmp_file_path = tmp_file.name
    # Push du modèle dans XCom pour le rendre disponible à la tâche suivante
    ti.xcom_push(key='trained_model', value=tmp_file_path)

def save_model_to_s3(**kwargs):

    aws_access_key_id = 'Your_key_id'
    aws_secret_access_key = 'The_secret_one1'
    ti = kwargs['ti']
    model_path = ti.xcom_pull(key='trained_model')
    with open(model_path,'rb') as model_file:
        model_data = BytesIO(model_file.read())
    fs = s3fs.S3FileSystem(key=aws_access_key_id,secret=aws_secret_access_key)
    s3_path = f"s3://path/vers/valo_model.cbm"
    with fs.open(s3_path,'wb') as f:
        f.write(model_data.getvalue())
    print(f"Model saved to S3 at {s3_path} successfully.")
    os.remove(model_path)

default_args = {
    'owner':'sabor',
    'retries': 5,
    'retry_delay' : timedelta(minutes=2)
}

with DAG (
    dag_id='ML_Modele',
    default_args=default_args,
    description = 'training our modele',
    start_date = datetime(2024,9,12),
    schedule_interval='@monthly',
    catchup=False,
) as dag:
    load_from_s3 = PythonOperator(
        task_id='load_from_s3',
        python_callable=load_from_s3
    )
    train_model = PythonOperator(
        task_id='train_model',
        python_callable=train_model
    )
    save_model_to_s3 = PythonOperator(
        task_id='save_model_to_s3',
        python_callable=save_model_to_s3
    )
    load_from_s3 >> train_model >> save_model_to_s3
