from util.api import insert_market_data, insert_twitter_data
from util.coins import coins

if __name__ == "__main__":
    for item in coins:
        insert_market_data(symbol=item['symbol'])
        insert_twitter_data(symbol=item['symbol'], query=item['query'])

