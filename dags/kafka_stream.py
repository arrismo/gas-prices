from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'moise',
    'start_date': datetime(2024,11,5,10,00)
}


def get_data():
    import json
    import requests

    url = "https://api.collectapi.com/gasPrice/stateUsaPrice"
    params = {
        "state": "NY"
    }
    headers = {
        "content-type": "application/json",
        "authorization": "apikey 12tgSwxkTOYdt6BNfbfK64:3khdxv0XlTN8rHA2YI2Xxo"
    }

    res = requests.get(url, params=params, headers=headers)
    res = res.json()
    res = res['result']['cities']
    return res


def format_data(res):
    data = []

    for location in res:
        processed_location = {
            'location': location['name'],
            'regular': float(location['gasoline']),
            'mid_grade': float(location['midGrade']),
            'premium': float(location['premium']),
            'diesel': float(location['diesel']),
            'unit': location['unit'],
            'currency': location['currency']
        }
        data.append(processed_location)

    return data


def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging


    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()
    while True:
        if time.time() > curr_time + 60:
            break
        try:
            res = get_data()
            res = format_data(res)

            producer.send('users_created', json.dumps(res).encode('utf-8'))

        except Exception as e:
            logging.error(f"An error occurred: {e}")
            continue



with DAG('user-automation',
         default_args = default_args,
         schedule='@daily',
         catchup = False) as dag:

    streaming_task = PythonOperator(
        task_id = 'stream_data_from_api',
        python_callable=stream_data
    )