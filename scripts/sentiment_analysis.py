from textblob import TextBlob
import pandas as pd

# Read the CSV file into a DataFrame
raw_df = pd.read_csv('data/output.csv')
pandas_df = pd.DataFrame(raw_df)

# Define the sentiment analysis function for a single description
def get_article_sentiment(description):
    analysis = TextBlob(description)  # Perform sentiment analysis on a single description (string)
    if analysis.sentiment.polarity > 0:
        return 'positive'
    elif analysis.sentiment.polarity == 0:
        return 'neutral'
    else:
        return 'negative'

# Apply the sentiment analysis function to each description in the DataFrame
pandas_df['sentiment'] = pandas_df['description'].apply(get_article_sentiment)

# View the DataFrame with the new sentiment column
#print(pandas_df[['description', 'sentiment']])

pandas_df.to_csv('data/output_with_sentiment.csv', index=False)