import pandas as pd
from textblob import TextBlob
from datetime import datetime
#import nltk
#nltk.download("punkt")

def get_article_sentiment(description):
    """
    Simple sentiment analysis function with extra cleaning
    """
    # Remove leading numbers and whitespace
    if isinstance(description, str):
        description = description.lstrip('0123456789 ').strip()
    
    # Recheck if description is a valid string after cleaning
    if not isinstance(description, str) or not description:
        return 'unknown'
    
    # Perform sentiment analysis
    analysis = TextBlob(description)
    sentiment_score = round(analysis.sentiment.polarity * 100, 2)
        
    if sentiment_score > 0:
        sentiment_label =  'positive'
    elif sentiment_score == 0:
        sentiment_label = 'neutral'
    else:
        sentiment_label = 'negative'
        
    return sentiment_score, sentiment_label


def get_word_count(title,word):
    # Simple, safe word counting method
    if not isinstance(title, str):
        return 0
    return title.lower().count(word.lower())
    


def main():
    # Read the CSV file into a DataFrame
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    # Read the CSV file, explicitly dropping the index column if present
    df = pd.read_csv(f'data/combined_output.csv')
    
    # Reset the index to ensure clean dataframe
    # df = df.reset_index(drop=True)
    
    # Apply sentiment analysis
    sentiment_results = df['description'].apply(get_article_sentiment)
    df['sentiment'] = sentiment_results.apply(lambda x: x[1])
    df['sentiment_score'] = sentiment_results.apply(lambda x: x[0])
    
    companies = [
        'google', 'openai', 'microsoft', 'tiktok', 
        'apple', 'amazon', 'nvidia', 'amd', 'anthropic'
    ]
    
    # Count company names in titles
    for company in companies:
        df[f'{company}_count'] = df['title'].apply(lambda x: get_word_count(x, company))
       
    
    
        
    # Save the updated DataFrame
    output_filename = f'data/output_with_sentiment.csv'
    df.to_csv(output_filename, index=False)

if __name__ == "__main__":
    main()