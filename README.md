# News Article Pipeline and Analysis



This project leverages the News API to collect recent articles about artificial intelligence, utilizing TextBlob for natural language processing and sentiment analysis. By extracting and analyzing news content, the pipeline processes articles to determine sentiment trends in AI-related reporting. Snowflake serves as the data warehouse, providing robust storage and scalability for large volumes of news data, while dbt (data build tool) enables efficient data transformation and modeling. The workflow involves retrieving news articles, performing sentiment analysis to categorize them as positive, negative, or neutral, and creating structured datasets that can reveal insights into media perception and narrative trends surrounding artificial intelligence technologies.

## Project Setup and Installation

### Prerequisites
- Python 3.8+
- Snowflake account
- [News API key](https://newsapi.org/)


### Installation Steps
1. Clone the repository:
git clone https://github.com/arrismo/news-dashboard.git
cd news-dashboard

### Create Virtual Environment 
`python -m venv venv`
`source venv/bin/activate`  # On Windows, use `venv\Scripts\activate`

### Install dependencies
`pip install -r requirements.txt`

### Set up environment variables:

# Create a .env file in the project root
`touch .env`

# Add the following:
`NEWS_API_KEY=your_news_api_key` \
`SNOWFLAKE_ACCOUNT=your_snowflake_account` \
`SNOWFLAKE_USER=your_username` \
`SNOWFLAKE_PASSWORD=your_password` 

### Running the pipeline

Extract News Data \
`python tap_news.py`

Run dbt transformations \
### Configurations

Update `news_project/models/` for custom transformations

`dbt run`



Generate Setiment analysis \
`python sentiment_analysis.py`

Load to Snowflake \
`python target-snowflake.py`

![Screenshot](diagram.png)