import pandas as pd
import boto3
import json
import psycopg2
from psycopg2.extras import execute_values
from io import StringIO


def get_secrets(secret_name):
    session = boto3.session.Session(region_name = 'ca-central-1')
    client = session.client(service_name = 'secretsmanager')
    response = client.get_secret_value(SecretId = secret_name)
    return json.loads(response['SecretString'])


def lambda_handler(event, context):
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']
    s3 = boto3.client('s3')
    file_obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    file_content = file_obj['Body'].read().decode('utf-8') 
    df = pd.read_csv(StringIO(file_content))
    secret_name ='database-conn'
    secrets = get_secrets(secret_name)
    db_credentials = secrets
    conn = psycopg2.connect(database = db_credentials['engine'],
                            user = db_credentials['username'],
                            password = db_credentials['password'],
                            host = db_credentials['host'],
                            port = db_credentials['port']
            )
    
    try:
        with conn.cursor() as cursor:
            query = "insert into public.stocks values %s"
            data_tuples = list(df.itertuples(index=False, name=None))
            execute_values(cursor, query, data_tuples)
            conn.commit()
            print("inserted complete")
    except Exception as e:
        print(f"error occuoured: {e}")
        conn.rollback()
    finally:
        conn.close()
