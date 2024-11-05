import http.client
import json
import csv
from datetime import datetime
from pymongo import MongoClient
from config import MONGO_URI, CRYPTO_DB, USERNAME, PASSWORD, API_KEY, TWITTER_DB

client = MongoClient(MONGO_URI, username=USERNAME, password=PASSWORD)
crypto_db = client[CRYPTO_DB]
crypto_collection = crypto_db["crypto"]

twitter_db = client[TWITTER_DB]
twitter_collection = twitter_db["twitter"]

def insert_market_data(symbol="BTC"):
  try:
    # API call
    conn = http.client.HTTPSConnection("real-time-quotes1.p.rapidapi.com")
    headers = {
        'x-rapidapi-key': API_KEY,
        'x-rapidapi-host': "real-time-quotes1.p.rapidapi.com"
    }
    conn.request("GET", f"/api/v1/realtime/crypto?source={symbol}&target=USD", headers=headers)
    res = conn.getresponse()
    data = res.read().decode("utf-8")
    parsed_data = json.loads(data)
        
    if parsed_data and len(parsed_data) > 0:
      price = parsed_data[0]['price']
      name = parsed_data[0]['name']
      dayHigh = parsed_data[0]['dayHigh']
      dayLow = parsed_data[0]['dayLow']
      volume = parsed_data[0]['volume']
      open = parsed_data[0]['open']
      close = parsed_data[0]['previousClose']
      timestamp = parsed_data[0]['timestamp']
      current_time = datetime.now()
      # Prepare data for insertion
      insert_data = {
          "symbol": symbol,
          "name": name,
          "price": price,
          "dayHigh": dayHigh,
          "dayLow": dayLow,
          "volume": volume,
          "open": open,
          "close": close,
          "timestamp": timestamp,
          "time": current_time,
      }
      
      print(f"Inserting data: {insert_data}")
      crypto_collection.insert_one(insert_data)
    else:
        print("No data received for the symbol")
    
  except Exception as e:
    print(f"Error in coin market API: {str(e)}")


def insert_twitter_data(query="bitcoin", symbol="BTC"):

  try:
    # API call
    conn = http.client.HTTPSConnection("twitter-api45.p.rapidapi.com")

    headers = {
        'x-rapidapi-key': API_KEY,
        'x-rapidapi-host': "twitter-api45.p.rapidapi.com"
    }

    conn.request("GET", f"/search.php?query={query}&search_type=Top", headers=headers)

    res = conn.getresponse()
    data = res.read().decode("utf-8")
    parsed_data = json.loads(data)['timeline']

    insert_data = []
    if parsed_data and len(parsed_data) > 0:
      for item in parsed_data:
        tweet_id = item['tweet_id']
        screen_name = item['screen_name']
        bookmarks = item['bookmarks']
        favorites = item['favorites']
        created_at = item["created_at"]
        text = item['text']
        lang = item['lang']
        quotes = item['quotes']
        replies = item['replies']
        retweets = item['retweets']
        current_time = datetime.now()

        insert_data.append({
          "symbol": symbol,
          "name": query,
          'tweet_id': tweet_id,
          'screen_name': screen_name,
          'bookmarks': bookmarks,
          'favorites': favorites,
          'created_at': created_at,
          'text': text,
          'lang': lang,
          'quotes': quotes,
          'replies': replies,
          'retweets': retweets,
          "time": current_time,
        })
      
      print(f"Inserting data: {insert_data}")
      twitter_collection.insert_many(insert_data)
    else:
        print("No data received for the symbol")
    
  except Exception as e:
    print(f"Error in twitter API: {str(e)}")


def download_data_to_csv(symbol="BTC"):
  query = {"symbol": symbol}
  try:
    result = list(twitter_collection.find(query).sort('timestamp', -1))
    with open('output.csv', 'w') as csv_file:
      writer = csv.DictWriter(csv_file, fieldnames=result[0].keys())
      writer.writeheader()
      writer.writerows(result)
  except Exception as e:
    print(f"Error in querying data: {str(e)}")



