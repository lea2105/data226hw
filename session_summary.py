from airflow.decorators import task
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
from datetime import timedelta
import logging
import snowflake.connector

def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_con')

    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

@task
def run_ctas(database, schema, table, select_sql, primary_key=None):

    logging.info(table)
    logging.info(select_sql)

    cur = return_snowflake_conn()

    try:
        sql = f"CREATE OR REPLACE TABLE {database}.{schema}.temp_{table} AS {select_sql}"
        logging.info(sql)
        cur.execute(sql)

        # do primary key uniquess check
        if primary_key is not None:
            sql = f"""
              SELECT {primary_key}, COUNT(1) AS cnt 
              FROM {database}.{schema}.temp_{table}
              GROUP BY 1
              ORDER BY 2 DESC
              LIMIT 1"""
            print(sql)
            cur.execute(sql)
            result = cur.fetchone()
            print(result, result[1])
            if int(result[1]) > 1:
                print("!!!!!!!!!!!!!!")
                raise Exception(f"Primary key uniqueness failed: {result}")
            
        main_table_creation_if_not_exists_sql = f"""
            CREATE TABLE IF NOT EXISTS {database}.{schema}.{table} AS
            SELECT * FROM {database}.{schema}.temp_{table} WHERE 1=0;"""
        cur.execute(main_table_creation_if_not_exists_sql)

        swap_sql = f"""ALTER TABLE {database}.{schema}.{table} SWAP WITH {database}.{schema}.temp_{table};"""
        cur.execute(swap_sql)
    except Exception as e:
        raise


with DAG(
    dag_id = 'session_summary',
    start_date = datetime.now(),
    catchup=False,
    tags=['ELT'],
    schedule = '45 2 * * *'
) as dag:

    database = "dev"
    schema = "analytics"
    table = "session_summary"
    select_sql = """
        WITH unified_data AS (
            SELECT 
                u.userId, 
                u.sessionId, 
                u.channel, 
                NULL AS ts
            FROM user_db_marmot.dev.user_session_channel u
            UNION ALL
            SELECT 
                NULL AS userId, 
                t.sessionId, 
                NULL AS channel, 
                t.ts
            FROM user_db_marmot.dev.session_timestamp t
        ),
        deduplicated_data AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY sessionId ORDER BY ts DESC NULLS LAST) AS row_num
            FROM unified_data
        )
        SELECT userId, sessionId, channel, ts
        FROM deduplicated_data
        WHERE row_num = 1;
        """

    run_ctas(database, schema, table, select_sql, primary_key='sessionId')
