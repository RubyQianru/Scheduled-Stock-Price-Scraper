import http.client
import json
from datetime import datetime
from pymongo import MongoClient
from config import MONGO_URI, DB_NAME, USERNAME, PASSWORD, API_KEY

client = MongoClient(MONGO_URI, username=USERNAME, password=PASSWORD)
db = client[DB_NAME]
collection = db["crypto"]

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
      collection.insert_one(insert_data)
    else:
        print("No data received for the symbol")
    
  except Exception as e:
    print(f"Error in coin market API: {str(e)}")