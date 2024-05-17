import redis
import duckdb
import os
import json
import logging
import tempfile

# Set up logging
logging.basicConfig(
    filename="app.log",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s:%(message)s",
)


redis_conn = redis.Redis(host="localhost", port=6379)
duckdb_conn = duckdb.connect()
duckdb_conn.execute("INSTALL json;")
duckdb_conn.execute("LOAD json;")

os.makedirs("data", exist_ok=True)

STREAM_NAME = "ingeststream"
CONSUMER_GROUP = "ingestgroup"
CONSUMER_NAME = "consumer1"

# Create a consumer group if it doesn't exist
try:
    redis_conn.xgroup_create(STREAM_NAME, CONSUMER_GROUP, id="0", mkstream=True)
except redis.exceptions.ResponseError as e:
    if "BUSYGROUP Consumer Group name already exists" not in str(e):
        raise


def consume_queue():
    while True:
        try:
            # Read data from the Redis stream
            messages = redis_conn.xreadgroup(
                CONSUMER_GROUP, CONSUMER_NAME, {STREAM_NAME: ">"}, count=1, block=5000
            )

            if messages:
                for message in messages:
                    stream, message_list = message
                    for msg in message_list:
                        message_id, data = msg
                        json_str = data[b"data"].decode("utf-8").replace("'", '"')
                        logging.info(f"Processing data: {json_str}")

                        try:
                            json_data = json.loads(json_str)
                        except json.JSONDecodeError as e:
                            logging.error(f"Error decoding JSON: {e}")
                            continue

                        # Write JSON data to a temporary file
                        with tempfile.NamedTemporaryFile(
                            delete=False, suffix=".json"
                        ) as temp_file:
                            temp_file.write(json.dumps(json_data).encode("utf-8"))
                            temp_file_path = temp_file.name

                        #  Insert JSON data into DuckDB
                        duckdb_conn.execute("ATTACH DATABASE '/data/duckdb.db' AS db1;")
                        logging.info("Database attached successfully")
                        duckdb_conn.execute("CREATE SCHEMA IF NOT EXISTS db1.ingest;")

                        query = f"""
                        CREATE  OR REPLACE TABLE  raw AS 
                            SELECT * FROM read_json_auto('{temp_file_path}');
                            
                        """
                        duckdb_conn.execute(query)
                        duckdb_conn.execute(""" CREATE TABLE IF NOT EXISTS  db1.ingest.messages AS SELECT * FROM raw; 
                                            CREATE OR REPLACE TABLE db1.ingest.messages AS 
                                            ( SELECT * FROM raw UNION SELECT * FROM db1.ingest.messages);
                                            
                                    """)
                        
                        
                        duckdb_conn.execute("DETACH DATABASE db1;")
                        


                        logging.info("Data ingested successfully")

                        os.remove(temp_file_path)
                        # Acknowledge the message
                        redis_conn.xack(STREAM_NAME, CONSUMER_GROUP, message_id)
        except Exception as e:
            logging.error(f"Error processing data: {e}")


if __name__ == "__main__":
    consume_queue()
