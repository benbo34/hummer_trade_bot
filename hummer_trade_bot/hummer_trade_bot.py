import threading
import time
import ccxt
import redis
import json

# Initialize the exchange (replace 'binance' with the exchange you want to use)
exchange = ccxt.binance({
    'enableRateLimit': True,
})

# Initialize Redis connection
redis_db = redis.StrictRedis(host='localhost', port=6379, db=0)

# Function to subscribe to WebSocket stream and update Redis with price data
def stream_prices(symbol):
    def on_message(ws, message):
        try:
            data = json.loads(message)
            if 'data' in data:
                ticker = data['data']
                if ticker['s'] == symbol:
                    redis_db.set(symbol, json.dumps(ticker))
        except Exception as e:
            print(f"Error processing WebSocket message: {e}")

    def on_error(ws, error):
        print(f"WebSocket error occurred: {error}")

    def on_close(ws):
        print("WebSocket connection closed")

    def on_open(ws):
        print("WebSocket connection opened")
        ws.send(json.dumps({
            "method": "SUBSCRIBE",
            "params": [f"{symbol.lower()}@ticker"],
            "id": 1
        }))

    websocket_url = 'wss://stream.binance.com:9443/ws/'
    ws = ccxt.async_support.websocket(wsurl=websocket_url, on_message=on_message,
                                       on_error=on_error, on_close=on_close)
    ws.on_open = on_open
    ws.run_forever()

# Function to get market data from Redis
def get_market_data(symbol):
    ticker_json = redis_db.get(symbol)
    if ticker_json:
        return json.loads(ticker_json)
    return None

# Function to calculate spread
def calculate_spread(ticker):
    if ticker:
        bid_price = float(ticker['b'])  # Highest current buy price
        ask_price = float(ticker['a'])  # Lowest current sell price
        spread = ask_price - bid_price
        return spread
    return None

# Function to place orders
def place_orders(symbol, spread):
    buy_threshold = 10  # Threshold for placing buy orders
    sell_threshold = 20  # Threshold for placing sell orders
    
    if spread is not None:
        if spread < buy_threshold:
            # Place a buy order
            print(f"Placing buy order for {symbol} with spread {spread}")
            # Example: exchange.create_limit_buy_order(symbol, amount, price)
        elif spread > sell_threshold:
            # Place a sell order
            print(f"Placing sell order for {symbol} with spread {spread}")
            # Example: exchange.create_limit_sell_order(symbol, amount, price)
        else:
            # Do nothing if spread is within thresholds
            print(f"No trading action taken for {symbol} with spread {spread}")
    else:
        print("Unable to place orders. Spread data is not available.")

    print(f"Spread for {symbol}: {spread}")

# Main function
def main():
    symbol = 'BTCUSDT'  # Example trading pair
    # Start WebSocket stream in a separate thread
    websocket_thread = threading.Thread(target=stream_prices, args=(symbol,))
    websocket_thread.start()

    while True:
        try:
            # Get market data from Redis
            ticker = get_market_data(symbol)
            spread = calculate_spread(ticker)
            place_orders(symbol, spread)
            time.sleep(5)  # Wait for 5 seconds before checking again
        except Exception as e:
            print(f"An error occurred: {e}")
            time.sleep(60)  # If an error occurs, wait for 60 seconds before retrying

if __name__ == "__main__":
    main()
