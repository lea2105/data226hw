from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import requests

def return_snowflake_conn():
    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_con')

    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

@task
def extract(url):
    response = requests.get(url)
    response.raise_for_status()  # This will raise an error for a failed request
    return response.json()

@task
def transform(data):
    results = []
    for d in data["Time Series (Daily)"]:
        stock_data = data["Time Series (Daily)"][d]
        stock_data["date"] = d
        results.append(stock_data)

        if len(results) >= 90:
            break

    return results

@task
def load_stock_data(con, records):
    staging_table = "dev.raw.temp_stock_data"
    target_table = "dev.raw.stock_data"

    try:
        con.execute("BEGIN;")
        con.execute(f"""
            CREATE OR REPLACE TABLE {target_table} (
                symbol VARCHAR,
                open DECIMAL(10, 2),
                close DECIMAL(10, 2),
                high DECIMAL(10, 2),
                low DECIMAL(10, 2),
                volume BIGINT,
                date DATE,
                PRIMARY KEY (symbol, date)
            );
        """)

        con.execute(f"""
            CREATE OR REPLACE TABLE {staging_table} (
                symbol VARCHAR,
                open DECIMAL(10, 2),
                close DECIMAL(10, 2),
                high DECIMAL(10, 2),
                low DECIMAL(10, 2),
                volume BIGINT,
                date DATE,
                PRIMARY KEY (symbol, date)
            );
        """)

        con.execute(f'DELETE FROM {target_table}')
        con.execute(f'DELETE FROM {staging_table}')

        # Insert the incoming stock data into the staging table
        for r in records:
            open_price = r['1. open']
            close_price = r['4. close']
            high_price = r['2. high']
            low_price = r['3. low']
            volume = r['5. volume']
            date = r['date']

            sql = f"""
                INSERT INTO {staging_table} (symbol, open, close, high, low, volume, date)
                VALUES ('AAPL', {open_price}, {close_price}, {high_price}, {low_price}, {volume}, '{date}');
            """
            con.execute(sql)

        # Perform the MERGE operation to upsert data from the staging table into the target table
        upsert_sql = f"""
            MERGE INTO {target_table} AS target
            USING {staging_table} AS stage
            ON target.symbol = stage.symbol AND target.date = stage.date
            WHEN MATCHED THEN
                UPDATE SET
                    target.open = stage.open,
                    target.close = stage.close,
                    target.high = stage.high,
                    target.low = stage.low,
                    target.volume = stage.volume
            WHEN NOT MATCHED THEN
                INSERT (symbol, open, close, high, low, volume, date)
                VALUES (stage.symbol, stage.open, stage.close, stage.high, stage.low, stage.volume, stage.date);
        """
        con.execute(upsert_sql)

        con.execute("COMMIT;")

    except Exception as e:
        con.execute("ROLLBACK;")
        print(f"Error: {e}")
        raise e

# Airflow DAG setup
with DAG(
    dag_id="stock_data_pipeline",
    start_date=datetime.now(),  # Set start_date to the current time, which makes the DAG trigger immediately
    catchup=False,
    tags=['ETL', 'stock'],
    schedule_interval=None  # Set schedule_interval to None to allow manual triggering
) as dag:
    
    target_table = 'dev.raw.stock_data'
    api_key = Variable.get('api_key', default_var = "")

    symbol = "AAPL"  
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}'

    data = extract(url)

    lines = transform(data)
    conn = return_snowflake_conn()
    load_stock_data(conn, lines)