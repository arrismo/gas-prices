import snowflake.connector
import os
from dotenv import load_dotenv
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime


current_date = datetime.now().strftime("%Y-%m-%d")
df = pd.read_csv(f'data/output_with_sentiment{current_date}.csv')
df = pd.DataFrame(df)
df.columns = df.columns.str.upper()

USER = os.getenv('USER')
PASSWORD = os.getenv('PASSWORD')
ACCOUNT = os.getenv('ACCOUNT')
WAREHOUSE = os.getenv('WAREHOUSE')
DATABASE = os.getenv('DATABASE')
SCHEMA = os.getenv('SCHEMA')

conn = snowflake.connector.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT,
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA
    )


success, nchunks, nrows, _ = write_pandas(
    conn, 
    df, 
    table_name='ARTICLEDATA',  
    database=conn.database,
    schema=conn.schema
)


conn.close()