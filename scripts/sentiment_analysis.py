import pandas as pd
from textblob import TextBlob
from datetime import datetime

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
    
    if analysis.sentiment.polarity > 0:
        return 'positive'
    elif analysis.sentiment.polarity == 0:
        return 'neutral'
    else:
        return 'negative'

def main():
    # Read the CSV file into a DataFrame
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    # Read the CSV file, explicitly dropping the index column if present
    df = pd.read_csv(f'data/output_{current_date}.csv', index_col=0)
    
    # Reset the index to ensure clean dataframe
    df = df.reset_index(drop=True)
    
    # Apply sentiment analysis
    df['sentiment'] = df['description'].apply(get_article_sentiment)
    
    # Save the updated DataFrame
    output_filename = f'data/output_with_sentiment_{current_date}.csv'
    df.to_csv(output_filename, index=False)
    
    # Print sentiment su""" mmary
    # print("Sentiment Analysis Summary:")
    # print(df['sentiment'].value_counts())
    
    # # Optional: Print detailed sentiment analysis
    # print("\nDetailed Sentiment Breakdown:")
    # for index, row in df.iterrows():
    #     print(f"\nTitle: {row['title']}")
    #     print(f"Description: {row['description']}")
    #     print(f"Sentiment: {row['sentiment']}") """

if __name__ == "__main__":
    main()