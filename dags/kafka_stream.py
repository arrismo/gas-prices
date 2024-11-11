from datetime import datetime


from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'moise',
    'start_date': datetime(2024,11,11,10,00)
}

def get_data():
    import json
    import requests
    url = ('https://newsapi.org/v2/everything?'
           'q=Apple&'
           'from=2024-11-09&'
           'sortBy=popularity&'
           'apiKey=afd67a6c7063461fa98d390ac2b40500')

    response = requests.get(url)
    response = response.json()
    response = response['articles'][1:]
    return response

def format_data(response):
    from datetime import datetime
    data = []

    for article in response:
        published_at = article['publishedAt']
        publish_date = datetime.strptime(published_at, '%Y-%m-%dT%H:%M:%SZ')
        formatted_publish_date = publish_date.strftime('%Y-%m-%d %H:%M:%S')

        processed_article = {
            'source':article['source']['name'],
            'author':article["author"],
            'title':article['title'],
            'publish_date':formatted_publish_date
        }
        data.append(processed_article)
    return data

def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging


    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000, api_version = (2,0,2))
    curr_time = time.time()
    while True:
        if time.time() > curr_time + 60:
            break
        try:
            res = format_data(get_data())
            print(json.dumps(res,indent=3))

            producer.send('users_created', json.dumps(res).encode('utf-8'))

            time.sleep(2)

        except Exception as e:
            logging.error(f"An error occurred: {e}")
            continue

#stream_data()

with DAG('user-automation',
         default_args = default_args,
         schedule='@daily',
         catchup = False) as dag:

    streaming_task = PythonOperator(
        task_id = 'stream_data_from_api',
        python_callable=stream_data
    )