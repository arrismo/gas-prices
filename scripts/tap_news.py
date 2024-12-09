import pandas as pd
import sys
import os
import json
from dotenv import load_dotenv
from datetime import datetime
import requests



def get_data():
    api_key = os.getenv('NEWS_API_KEY')
    response = requests.get(f'https://newsapi.org/v2/everything?q=artificial&q=AI&language=en&apiKey={api_key}')
    response = response.json()
    response = response['articles'][1:]
    return response

def format_data(response):
    
    data = []

    for article in response:
        if article['source']['name'] == '[Removed]':
            continue
        
        published_at = article['publishedAt']
        publish_date = datetime.strptime(published_at, '%Y-%m-%dT%H:%M:%SZ')
        formatted_publish_date = publish_date.strftime('%Y-%m-%d %H:%M:%S')

        processed_article = {
            'source':article['source']['name'],
            'author':article["author"],
            'title':article['title'],
            'description':article['description'],
            'publish_date':formatted_publish_date
        }
        data.append(processed_article)
    df_list = pd.DataFrame(data)    
    current_date = datetime.now().strftime("%Y-%m-%d")
    df_list.to_csv(f'output{current_date}.csv',index= False)
    return data


def run():
    return format_data(get_data())


def main():
    data = run()
    

main()