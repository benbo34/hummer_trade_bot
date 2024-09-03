import asyncio
import json
import redis
import websockets

# Redis connection setup
redis_host = "redis"
redis_port = 6379
redis_client = redis.Redis(host=redis_host, port=redis_port, db=0)

# WebSocket endpoint for crypto exchange (example: Binance)
websocket_url = "wss://stream.binance.com:9443/ws/btcusdt@trade"

async def ingest_market_data():
    async with websockets.connect(websocket_url) as websocket:
        while True:
            message = await websocket.recv()
            data = json.loads(message)
            
            # Process and store the data in Redis
            redis_client.set(f"market_data:{data['s']}", json.dumps(data))
            print(f"Stored data in Redis: {data}")

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(ingest_market_data())
