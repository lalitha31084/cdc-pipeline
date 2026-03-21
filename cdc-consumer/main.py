import time
import sys
from decimal import Decimal

import meilisearch
import psycopg2
import psycopg2.extras

# Force logs to show up immediately
sys.stdout.reconfigure(line_buffering=True)

DB_CONF = "host=postgres dbname=cdc_db user=postgres password=postgres"
MEILI_URL = "http://meilisearch:7700"
MEILI_KEY = "masterKey"
MEILI_INDEX = "products"
SLOT_NAME = "cdc_slider"
PUBLICATION_NAME = "my_publication"

MEILI_CLIENT = meilisearch.Client(MEILI_URL, MEILI_KEY)
last_sync_time = 0.0


def clean_data(value):
    if isinstance(value, list):
        return [clean_data(item) for item in value]
    if isinstance(value, dict):
        return {key: clean_data(item) for key, item in value.items()}
    if isinstance(value, Decimal):
        return float(value)
    return value


def fetch_products():
    conn = psycopg2.connect(DB_CONF)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT product_id, name, description, price, category_id FROM products ORDER BY product_id"
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    cleaned_rows = []
    for row in rows:
        clean_row = clean_data(row)
        clean_row["id"] = clean_row["product_id"]
        cleaned_rows.append(clean_row)

    return cleaned_rows


def sync_products_to_meili():
    products = fetch_products()
    if not products:
        return 0

    index = MEILI_CLIENT.index(MEILI_INDEX)
    index.add_documents(products)
    return len(products)


def start_cdc():
    global last_sync_time

    print("🚀 CDC Consumer: Starting logical stream handler...")

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
                conn.rollback()

            cur.start_replication(
                slot_name=SLOT_NAME,
                options={"proto_version": "1", "publication_names": PUBLICATION_NAME},
                decode=False,
            )
            print(f"✅ SUCCESS: Connected to slot '{SLOT_NAME}'")

            def consume_logs(msg):
                global last_sync_time
                now = time.time()
                print("✅ Change Captured")

                if now - last_sync_time > 1.0:
                    synced = sync_products_to_meili()
                    print(f"✅ FINAL SYNC SUCCESS: {synced} rows sent to Meilisearch")
                    last_sync_time = now

                msg.cursor.send_feedback(flush_lsn=msg.data_start)

            cur.consume_stream(consume_logs)

        except Exception as e:
            print(f"❌ Stream Error: {e}")
            print("⏳ Retrying in 5 seconds...")
            time.sleep(5)


if __name__ == "__main__":
    start_cdc()
