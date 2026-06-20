import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE')
)

cur = conn.cursor()
cur.execute("SHOW TABLES IN DATABASE egypt_jobs_db")
for row in cur.fetchall():
    print(row[1], "|", row[3])  # table name | schema name

conn.close()