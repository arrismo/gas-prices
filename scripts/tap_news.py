import pandas as pd
import sys
import os
import json
from dotenv import load_dotenv
from datetime import datetime
import requests

def get_data(max_pages=5):
    """
    Fetch news articles about AI across multiple pages
    
    :param max_pages: Maximum number of pages to fetch
    :return: List of articles from all pages
    """
    api_key = os.getenv('NEWS_API_KEY')
    if not api_key:
        raise ValueError("NEWS_API_KEY environment variable not set")
    
    all_articles = []
    
    for page in range(1, max_pages + 1):
        try:
            # Construct the API request with pagination
            params = {
                'q': 'artificial intelligence OR AI', 
                'language': 'en',
                'apiKey': api_key,
                'page': page
                }
            
            response = requests.get('https://newsapi.org/v2/everything', params=params)
            
            # Check for successful API response
            if response.status_code != 200:
                print(f"Error fetching page {page}: HTTP {response.status_code}")
                break
            
            data = response.json()
            
            # Break if no more articles
            if not data['articles']:
                break
            
            all_articles.extend(data['articles'])
            
            print(f"Fetched page {page}, total articles: {len(all_articles)}")
        
        except requests.RequestException as e:
            print(f"Request error on page {page}: {e}")
            break
    
    return all_articles[1:]  # Skip the first article as in your original code

def format_data(response):
    """
    Process and format articles into a DataFrame and save to CSV
    
    :param response: List of articles
    :return: Processed data
    """
    data = []
    for article in response:
        # Skip removed sources
        if article['source']['name'] == '[Removed]':
            continue
        
        try:
            # Safely handle potential missing or malformed data
            published_at = article.get('publishedAt', '')
            if published_at:
                publish_date = datetime.strptime(published_at, '%Y-%m-%dT%H:%M:%SZ')
                formatted_publish_date = publish_date.strftime('%Y-%m-%d %H:%M:%S')
            else:
                formatted_publish_date = ''
            
            processed_article = {
                'source': article['source'].get('name', 'Unknown'),
                'author': article.get('author', 'Unknown'),
                'title': article.get('title', ''),
                'description': article.get('description', ''),
                'publish_date': formatted_publish_date
            }
            data.append(processed_article)
        
        except (ValueError, KeyError) as e:
            print(f"Error processing article: {e}")
    
    # Create DataFrame
    df_list = pd.DataFrame(data)
    
    # Save to CSV with current date
    current_date = datetime.now().strftime("%Y-%m-%d")
    output_filename = f'output_{current_date}.csv'
    df_list.to_csv(output_filename, index=False)
    print(f"Saved {len(df_list)} articles to {output_filename}")
    
    return data

def run(max_pages=5):
    """
    Main run function to fetch and process data
    
    :param max_pages: Maximum number of pages to fetch
    :return: Processed data
    """
    try:
        # Load environment variables
        load_dotenv()
        
        # Fetch and process data
        articles = get_data(max_pages)
        return format_data(articles)
    
    except Exception as e:
        print(f"An error occurred: {e}")
        return []

def main():
    """
    Entry point of the script
    """
    data = run()
    
if __name__ == "__main__":
    main()