import time
import sys
import os
import struct
import json
import redis
import meilisearch
import psycopg2
import psycopg2.extras
from decimal import Decimal

# Force logs to show up immediately
sys.stdout.reconfigure(line_buffering=True)

POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "cdc_db")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
MEILI_URL = os.getenv("MEILI_URL", "http://meilisearch:7700")
MEILI_KEY = os.getenv("MEILI_MASTER_KEY", "masterKey")

DB_CONF = f"host={POSTGRES_HOST} dbname={POSTGRES_DB} user={POSTGRES_USER} password={POSTGRES_PASSWORD}"

MEILI_INDEX = "products"
SLOT_NAME = "cdc_slider"
PUBLICATION_NAME = "my_publication"

MEILI_CLIENT = meilisearch.Client(MEILI_URL, MEILI_KEY)

redis_client = redis.Redis(host='redis', port=6379, db=0)

CHECKPOINT_FILE = "lsn_checkpoint.txt"

def get_last_lsn():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            content = f.read().strip()
            if content:
                try:
                    # Convert X/Y string to format that can be used or use it directly
                    return content
                except Exception:
                    return "0/0"
    return "0/0"

def write_lsn(lsn):
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(lsn)

def clean_data(value):
    if isinstance(value, list):
        return [clean_data(item) for item in value]
    if isinstance(value, dict):
        return {key: clean_data(item) for key, item in value.items()}
    if isinstance(value, Decimal):
        return float(value)
    return value

relation_cache = {}

def parse_tuple_data(data, columns):
    index = 0
    num_columns = struct.unpack(">H", data[index:index+2])[0]
    index += 2
    row_data = {}
    for i in range(num_columns):
        col_type = data[index:index+1].decode('ascii')
        index += 1
        if col_type == 't':
            col_len = struct.unpack(">I", data[index:index+4])[0]
            index += 4
            col_val = data[index:index+col_len].decode('utf-8')
            index += col_len
            if columns and i < len(columns):
                row_data[columns[i]] = col_val
        elif col_type == 'n':
            if columns and i < len(columns):
                row_data[columns[i]] = None
        elif col_type == 'u':
            if columns and i < len(columns):
                row_data[columns[i]] = None
    return row_data

def consume_logs(msg):
    # pgoutput format parser
    data = msg.data
    msg_type = data[0:1].decode('ascii')
    
    if msg_type == 'R':
        # Relation message
        index = 1
        rel_id = struct.unpack(">I", data[index:index+4])[0]
        index += 4
        ns_len = data.find(b'\0', index) - index
        ns = data[index:index+ns_len].decode('utf-8')
        index += ns_len + 1
        rel_name_len = data.find(b'\0', index) - index
        rel_name = data[index:index+rel_name_len].decode('utf-8')
        index += rel_name_len + 1
        replica_id = data[index:index+1]
        index += 1
        num_columns = struct.unpack(">H", data[index:index+2])[0]
        index += 2
        columns = []
        for _ in range(num_columns):
            key_flag = data[index:index+1]
            index += 1
            col_name_len = data.find(b'\0', index) - index
            col_name = data[index:index+col_name_len].decode('utf-8')
            index += col_name_len + 1
            type_id = struct.unpack(">I", data[index:index+4])[0]
            index += 4
            modifier = struct.unpack(">I", data[index:index+4])[0]
            index += 4
            columns.append(col_name)
        relation_cache[rel_id] = {'name': rel_name, 'columns': columns}
        
    elif msg_type == 'I':
        # Insert message
        index = 1
        rel_id = struct.unpack(">I", data[index:index+4])[0]
        index += 4
        new_tuple_type = data[index:index+1].decode('ascii')
        index += 1
        if new_tuple_type == 'N':
            rel_info = relation_cache.get(rel_id)
            if rel_info and rel_info['name'] == 'products':
                cols = rel_info.get('columns')
                row_data = parse_tuple_data(data[index:], cols)
                if row_data:
                    clean_row = clean_data(row_data)
                    if 'product_id' in clean_row:
                        clean_row['id'] = clean_row['product_id']
                        try:
                            MEILI_CLIENT.index(MEILI_INDEX).add_documents([clean_row])
                            print(f"✅ Inserted product: {clean_row.get('id')}")
                            redis_client.publish('cdc_events', json.dumps({"table": "products", "operation": "INSERT", "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}))
                        except Exception as e:
                            print(f"Error inserting: {e}")

    elif msg_type == 'U':
        # Update message
        index = 1
        rel_id = struct.unpack(">I", data[index:index+4])[0]
        index += 4
        # We might have old tuple, depending on REPLICA IDENTITY FULL
        tuple_type = data[index:index+1].decode('ascii')
        index += 1
        if tuple_type == 'O' or tuple_type == 'K':
            # Skip old tuple
            num_columns = struct.unpack(">H", data[index:index+2])[0]
            index += 2
            for i in range(num_columns):
                col_type = data[index:index+1].decode('ascii')
                index += 1
                if col_type == 't':
                    col_len = struct.unpack(">I", data[index:index+4])[0]
                    index += 4 + col_len
                elif col_type == 'n' or col_type == 'u':
                    pass
            tuple_type = data[index:index+1].decode('ascii')
            index += 1
            
        if tuple_type == 'N':
            rel_info = relation_cache.get(rel_id)
            if rel_info and rel_info['name'] == 'products':
                cols = rel_info.get('columns')
                row_data = parse_tuple_data(data[index:], cols)
                if row_data:
                    clean_row = clean_data(row_data)
                    if 'product_id' in clean_row:
                        clean_row['id'] = clean_row['product_id']
                        try:
                            MEILI_CLIENT.index(MEILI_INDEX).update_documents([clean_row])
                            print(f"✅ Updated product: {clean_row.get('id')}")
                            redis_client.publish('cdc_events', json.dumps({"table": "products", "operation": "UPDATE", "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}))
                        except Exception as e:
                            print(f"Error updating: {e}")

    elif msg_type == 'D':
        # Delete message
        index = 1
        rel_id = struct.unpack(">I", data[index:index+4])[0]
        index += 4
        tuple_type = data[index:index+1].decode('ascii')
        index += 1
        if tuple_type == 'O' or tuple_type == 'K':
            rel_info = relation_cache.get(rel_id)
            if rel_info and rel_info['name'] == 'products':
                cols = rel_info.get('columns')
                row_data = parse_tuple_data(data[index:], cols)
                if row_data:
                    clean_row = clean_data(row_data)
                    product_id = clean_row.get('product_id')
                    if product_id is not None:
                        try:
                            MEILI_CLIENT.index(MEILI_INDEX).delete_document(str(product_id))
                            print(f"✅ Deleted product: {product_id}")
                            redis_client.publish('cdc_events', json.dumps({"table": "products", "operation": "DELETE", "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}))
                        except Exception as e:
                            print(f"Error deleting: {e}")
    
    elif msg_type == 'C':
        # Commit message, save LSN
        lsn = msg.data_start
        # LSN format conversion is tricky manually, but we can write the formatted one provided by psycopg2
        write_lsn(str(psycopg2.extensions.AsIs(lsn)))

    # Always reply keep-alive
    if msg.data_start:
         msg.cursor.send_feedback(flush_lsn=msg.data_start)


def fetch_start_lsn():
    lsn_str = get_last_lsn()
    if lsn_str != "0/0":
        return lsn_str
    return "0/0"

def start_cdc():
    print("🚀 CDC Consumer: Starting logical stream handler...")

    start_lsn_str = fetch_start_lsn()

    while True:
        try:
            conn = psycopg2.connect(
                DB_CONF,
                connection_factory=psycopg2.extras.LogicalReplicationConnection,
            )
            cur = conn.cursor()

            try:
                cur.create_replication_slot(SLOT_NAME, output_plugin="pgoutput")
                print(f"✅ Created slot '{SLOT_NAME}'")
            except psycopg2.errors.DuplicateObject:
                pass

            cur.start_replication(
                slot_name=SLOT_NAME,
                options={"proto_version": "1", "publication_names": PUBLICATION_NAME},
                decode=False,
                start_lsn=start_lsn_str
            )
            print(f"✅ SUCCESS: Connected to slot '{SLOT_NAME}' starting from {start_lsn_str}")

            cur.consume_stream(consume_logs)

        except Exception as e:
            print(f"� Error {e}. Retrying in 5 seconds...")
            time.sleep(5)


if __name__ == "__main__":
    start_cdc()
