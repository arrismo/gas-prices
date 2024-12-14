import snowflake.connector
import os
from dotenv import load_dotenv
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

df = pd.read_csv(f'data/output_with_sentiment.csv', sep=',', quotechar='"', header=0)
df['author'] = df['author'].str.replace('\n', ' ').str[:100]
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


# Create cursor
cursor = conn.cursor()

# Drop table if exists (to overwrite)
cursor.execute("DROP TABLE IF EXISTS ARTICLEDATA")

column_definitions = []
for column, dtype in df.dtypes.items():
    if pd.api.types.is_integer_dtype(dtype):
        sql_type = 'INTEGER'
    elif pd.api.types.is_float_dtype(dtype):
        sql_type = 'FLOAT'
    else:
        sql_type = 'VARCHAR(500)'  # Adjust length as needed
    column_definitions.append(f"{column} {sql_type}")

create_table_sql = f"CREATE TABLE ARTICLEDATA ({', '.join(column_definitions)})"
cursor.execute(create_table_sql)

success, nchunks, nrows, _ = write_pandas(
    conn, 
    df, 
    table_name='ARTICLEDATA',  
    database=conn.database,
    schema=conn.schema
)


conn.close()
print(f"Successfully wrote {nrows} rows to ARTICLEDATA")