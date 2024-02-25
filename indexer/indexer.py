from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import time
from pymongo import MongoClient
import os
import logging

# Logging configuration
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s — %(message)s',
                    datefmt='%Y-%m-%d_%H:%M:%S',
                    handlers=[logging.StreamHandler()])

# Configuration
BLOCK_TIME = 1  # Block time in seconds
START_BLOCK = 16335524
MONGO_URI = "mongodb://mongo:27017/"  # Adjust if you have a different MongoDB URI
DB_NAME = "blockchainDB"
COLLECTION_NAME = "transactions"
RPC_URL = os.getenv("RPC_URL", "http://node:8648/")  # Configurable RPC URL


def update_last_indexed_block(block_number, db):
    db.lastIndexedBlock.update_one(
        {"id": "lastIndexedBlock"},
        {"$set": {"block_number": block_number}},
        upsert=True
    )

def get_last_indexed_block(db):
    last_block_doc = db.lastIndexedBlock.find_one({"id": "lastIndexedBlock"})
    if last_block_doc:
        return last_block_doc['block_number']
    else:
        return None

def fetch_latest_block_number():
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getBlockNumber",
        "params": []
    }
    response = requests.post(RPC_URL, json=payload)
    if response.status_code == 200:
        data = response.json()
        return data['result']['data']
    else:
        return None

def fetch_genesis_block_number():
    payload = {
        "jsonrpc": "2.0",
        "method": "getPolicyConstants",
        "params": [],
        "id": 0
    }
    response = requests.post(RPC_URL, json=payload)
    if response.status_code == 200:
        data = response.json()
        return data['result']['data']['genesisBlockNumber']
    else:
        return None

def fetch_transactions_by_block(block_number):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransactionsByBlockNumber",
        "params": [block_number]
    }
    try:
        response = requests.post(RPC_URL, json=payload, timeout=10)  # Added timeout for the request
        response.raise_for_status()  # This will raise an exception for HTTP error responses
        data = response.json()
        if 'result' in data and 'data' in data['result']:
            return data['result']['data']
        else:
            logging.warning(f"No transaction data found for block {block_number}.")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed for block {block_number}: {e}")
        return None

def fetch_blocks_parallel(start_block, end_block, collection, latest_block, db, max_workers=25):
    blocks_range = range(start_block, end_block + 1)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_block = {executor.submit(fetch_transactions_by_block, block_number): block_number for block_number in blocks_range}
        for future in as_completed(future_to_block):
            block_number = future_to_block[future]
            try:
                transactions = future.result()
                if transactions:
                    collection.insert_many(transactions, ordered=False)
                    logging.info(f"{block_number} - Inserted {len(transactions)} transactions. Progress: {block_number}/{latest_block} ({latest_block - block_number} blocks left)")
                    update_last_indexed_block(block_number, db)
                else:
                    logging.debug(f"{block_number} - No transactions found for block")
            except pymongo.errors.BulkWriteError as e:
                logging.error(f"Error inserting transactions for block {block_number}: {e.details}")
                # Process successfully inserted transactions before the error occurred
                update_last_indexed_block(block_number, db)

def fetch_blocks_real_time(start_block, latest_block, collection, db):
    current_block = start_block
    while True:
        transactions = fetch_transactions_by_block(current_block)
        if transactions:
            collection.transactions.insert_many(transactions)
            logging.info(f"{current_block} - Inserted {len(transactions)} transactions. Progress: {current_block}/{latest_block} ({latest_block - current_block} blocks left)")
        else:
            logging.debug(f"{current_block} - No transactions found for block ")
        current_block += 1
        time.sleep(BLOCK_TIME)  # Respect the 1-second block time

def main():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    # Ensure the transactions collection has a unique index on the hash
    collection.create_index([("hash", 1)], unique=True)
    
    genesis_block = fetch_genesis_block_number()
    latest_block = fetch_latest_block_number()
    
    logging.info("Nimiq v2 TX Indexer")
    logging.info(f"Genesis block: {genesis_block}")
    logging.info(f"Latest block: {latest_block}")
    
    if genesis_block is None or latest_block is None:
        logging.error("Failed to fetch blockchain constants.")
        return
    
    last_indexed_block = get_last_indexed_block(db)
    current_block = last_indexed_block if last_indexed_block else (START_BLOCK if START_BLOCK > genesis_block else genesis_block)

    logging.info(f"Resuming from block: {current_block}")

    # Parallel fetching if we're significantly behind
    if latest_block - current_block > 25:  # Example threshold
        logging.info("Starting parallel fetching...")
        fetch_blocks_parallel(current_block, latest_block - 100, collection, latest_block, db, max_workers=20)
        current_block = latest_block - 99

    # Transition to real-time fetching
    logging.info("Switching to real-time fetching...")
    fetch_blocks_real_time(current_block, latest_block, collection, db)


if __name__ == "__main__":
    main()
