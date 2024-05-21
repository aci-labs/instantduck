import redis
import duckdb
import os
import json
import logging
import subprocess
import time
import fsspec

# Set up logging
logging.basicConfig(
    filename="app.log",
    level=logging.INFO,
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

def run_dbt_models(tags_list):
    command = ["dbt", "run"]
    for tag in tags_list:
        command.extend(["--models", f"tag:{tag}"])
    command.extend(["--project-dir", "/rt_analytics"])
    command.extend(["--profiles-dir", "/rt_analytics"])
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        logging.info(f"dbt models ran successfully: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logging.error(f"dbt run failed: {e.stderr}")

def process_batch(messages_batch):
    all_json_data = []

    for message in messages_batch:
        message_id, data = message
        json_str = data[b"data"].decode("utf-8").replace("'", '"')
        logging.info(f"Processing data")

        try:
            json_data = json.loads(json_str)
            all_json_data.append(json_data)
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON: {e}")
            continue    
    
    if not all_json_data:
        return

    duckdb_conn.execute("ATTACH DATABASE '/data/duckdb.db' AS db1;")
    logging.info("Database attached successfully")
    with fsspec.filesystem('memory').open(f'stream.json', 'w') as file:
            file.write(json.dumps(all_json_data))
    
    duckdb_conn.register_filesystem(fsspec.filesystem('memory'))

    #logging.info(f"Filesystem registered successfully : {duckdb_conn.list_filesystems()}", )

    query = f"""
    CREATE SCHEMA IF NOT EXISTS db1.ingest;
    CREATE OR REPLACE TABLE raw AS 
        SELECT * FROM read_json_auto('memory://stream.json');
    CREATE TABLE IF NOT EXISTS db1.ingest.messages AS SELECT * FROM raw;
    CREATE OR REPLACE TABLE db1.ingest.messages AS 
        (SELECT * FROM raw UNION SELECT * FROM db1.ingest.messages);
    """
    duckdb_conn.execute(query)
    duckdb_conn.execute("DETACH DATABASE db1;")

    logging.info("Data ingested successfully")

    for message in messages_batch:
        message_id, _ = message
        redis_conn.xack(STREAM_NAME, CONSUMER_GROUP, message_id)

    # Run dbt models
    if all_json_data and 'tags' in all_json_data[0]:
        tags_list = all_json_data[0]['tags'].split(',')
        run_dbt_models(tags_list)

def consume_queue(batch_size=10, max_wait_time=1):
    last_batch_time = time.time()
    messages_batch = []

    while True:
        try:
            # Read data from the Redis stream
            messages = redis_conn.xreadgroup(
                CONSUMER_GROUP, CONSUMER_NAME, {STREAM_NAME: ">"}, count=batch_size, block=5000
            )

            if messages:
                stream, message_list = messages[0]
                messages_batch.extend(message_list)

                # Check if batch size is reached or max wait time exceeded
                if len(messages_batch) >= batch_size or (time.time() - last_batch_time) >= max_wait_time:
                    logging.info(f"Processing batch of size: {len(messages_batch)}")

                    process_batch(messages_batch)
                    messages_batch = []
                    last_batch_time = time.time()

        except Exception as e:
            logging.error(f"Error processing data: {e}")


def process_message(message):
    message_id, data = message
    json_str = data[b"data"].decode("utf-8").replace("'", '"')
    logging.info(f"Processing data: {json_str}")

    try:
        json_data = json.loads(json_str)
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON: {e}")
        return None

    duckdb_conn.execute("ATTACH DATABASE '/data/duckdb.db' AS db1;")
    logging.info("Database attached successfully")
    # duckdb_conn.execute("CREATE SCHEMA IF NOT EXISTS db1.ingest;")

    with fsspec.filesystem('memory').open(f'stream.json', 'w') as file:
            file.write(json.dumps(json_data))
    
    duckdb_conn.register_filesystem(fsspec.filesystem('memory'))

    #logging.info(f"Filesystem registered successfully : {duckdb_conn.list_filesystems()}", )

    query = f"""
    CREATE SCHEMA IF NOT EXISTS db1.ingest;
    CREATE OR REPLACE TABLE raw AS 
        SELECT * FROM read_json_auto('memory://stream.json');
    CREATE TABLE IF NOT EXISTS db1.ingest.messages AS SELECT * FROM raw;
    CREATE OR REPLACE TABLE db1.ingest.messages AS 
        (SELECT * FROM raw UNION SELECT * FROM db1.ingest.messages);
    """
    duckdb_conn.execute(query)
    # duckdb_conn.execute("""
    #     CREATE TABLE IF NOT EXISTS db1.ingest.messages AS SELECT * FROM raw;
    #     CREATE OR REPLACE TABLE db1.ingest.messages AS 
    #     (SELECT * FROM raw UNION SELECT * FROM db1.ingest.messages);
    # """)

    duckdb_conn.execute("DETACH DATABASE db1;")

    logging.info("Data ingested successfully")
    redis_conn.xack(STREAM_NAME, CONSUMER_GROUP, message_id)

    # Run dbt models
    if 'tags' in json_data:
        tags_list = json_data['tags'].split(',')
        run_dbt_models(tags_list)

def unitary_consume_queue(batch_size=10, max_wait_time=1):
    last_batch_time = time.time()
    messages_batch = []

    while True:
        try:
            # Read data from the Redis stream
            messages = redis_conn.xreadgroup(
                CONSUMER_GROUP, CONSUMER_NAME, {STREAM_NAME: ">"}, count=batch_size, block=5000
            )

            if messages:
                stream, message_list = messages[0]
                messages_batch.extend(message_list)

                # Check if batch size is reached or max wait time exceeded
                if len(messages_batch) >= batch_size or (time.time() - last_batch_time) >= max_wait_time:
                    for msg in messages_batch:
                        process_message(msg)

                    messages_batch = []
                    last_batch_time = time.time()

        except Exception as e:
            logging.error(f"Error processing data: {e}")

if __name__ == "__main__":
    consume_queue()
