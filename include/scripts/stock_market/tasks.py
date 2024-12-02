from airflow.hooks.base import BaseHook
import requests
import json
from minio import Minio
from io import BytesIO, StringIO
import psycopg2

BUCKET_NAME = "stock-market"

def _get_stock_prices(url, symbol):
    url=f"{url}/{symbol}?interval=1d&range=1y"
    api=BaseHook.get_connection('stock_api')
    response=requests.get(url, headers=api.extra_dejson['headers'])
    return json.dumps(response.json()['chart']['result'][0])

def _get_minio_client():
    minio=BaseHook.get_connection('minio')
    client=Minio(
        endpoint=minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False
    )
    return client

def _store_prices(stock):
    minio_client=_get_minio_client()
    if not minio_client.bucket_exists(BUCKET_NAME):
        minio_client.make_bucket(BUCKET_NAME)
    stock=json.loads(stock)
    symbol=stock['meta']['symbol']
    data=json.dumps(stock).encode('utf-8')
    obj=minio_client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=f"{symbol}/prices.json",
        data=BytesIO(data),
        length=len(data),
        content_type="application/json"
    )
    return f"{obj.bucket_name}/{symbol}"

def _get_formatted_csv(stock_folder_path):
    minio_client = _get_minio_client()
    prefix_name = f"{stock_folder_path.split('/')[1]}/formatted_prices/"
    objects = minio_client.list_objects(BUCKET_NAME, prefix=prefix_name, recursive=True)
    for object in objects:
        if object.object_name.endswith(".csv"):
            return object.object_name
    return None 

def _get_postgres_connection():
    conn = psycopg2.connect(
        host=BaseHook.get_connection("postgres").host,
        dbname=BaseHook.get_connection("postgres").schema,
        user=BaseHook.get_connection("postgres").login,
        password=BaseHook.get_connection("postgres").password
    )
    return conn

def _load_to_dw(csv_path):
    minio_client = _get_minio_client()
    pg_conn = _get_postgres_connection()
    cur = pg_conn.cursor()
    response = minio_client.get_object(BUCKET_NAME, csv_path)
    next(response)

    cur.execute(
        """CREATE SCHEMA IF NOT EXISTS dw;""")
    cur.execute(
        """CREATE TABLE IF NOT EXISTS dw.stocks_prices(
        "timestamp" bigint,
        close float,
        high float,
        low float,
        open float,
        volume bigint,
        date date
        );""")
    
    cur.execute("""TRUNCATE TABLE dw.stocks_prices;""")
    copy_query = """COPY dw.stocks_prices ("timestamp", close, high, low, open, volume, date) FROM STDIN WITH CSV DELIMITER ','"""
    cur.copy_expert(copy_query, StringIO(response.read().decode("utf-8")))

    pg_conn.commit()
    cur.close()
    pg_conn.close()
    response.close()
    response.release_conn()
