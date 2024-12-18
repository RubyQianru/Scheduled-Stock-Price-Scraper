import csv
from pymongo import MongoClient
from config import MONGO_URI, CRYPTO_DB, USERNAME, PASSWORD, API_KEY, TWITTER_DB

client = MongoClient(MONGO_URI, username=USERNAME, password=PASSWORD)
crypto_db = client[CRYPTO_DB]
crypto_collection = crypto_db["crypto"]

twitter_db = client[TWITTER_DB]
twitter_collection = twitter_db["twitter"]

def download_data_to_csv(symbol="BTC"):
  query = {"symbol": symbol}
  try:
    twitter_result = list(twitter_collection.find(query).sort('timestamp', -1))
    crypto_result = list(crypto_collection.find(query).sort('timestamp', -1))

    with open('twitter_data.csv', 'w') as csv_file:
      twitter_writer = csv.DictWriter(csv_file, fieldnames=twitter_result[0].keys())
      twitter_writer.writeheader()
      twitter_writer.writerows(twitter_result)

    with open('crytpo_data.csv', 'w') as csv_file:
      crypto_writer = csv.DictWriter(csv_file, fieldnames=crypto_result[0].keys())
      crypto_writer.writeheader()
      crypto_writer.writerows(crypto_result)

  except Exception as e:
    print(f"Error in querying data: {str(e)}")