from django.shortcuts import render
import redis
import json
from datetime import datetime
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync

# Create your views here.

def market_data_view(request):
    # Redis connection
    redis_client = redis.Redis(host='localhost', port=6379, db=0)
    
    # Get the latest market data
    market_data = redis_client.get('market_data:BTCUSDT')
    
    if market_data:
        market_data = json.loads(market_data)
    else:
        market_data = {'error': 'No data available'}
    
    context = {
        'market_data': market_data,
    }
    
    return render(request, 'trading_data/market_data.html', context)

def time_series_view(request):
    # Redis connection
    redis_client = redis.Redis(host='localhost', port=6379, db=0)
    
    # Get historical trades
    historical_trades = redis_client.zrange('historical_trades:BTCUSDT', 0, -1, withscores=True)
    
    # Process the data for the chart
    timestamps = []
    prices = []
    quantities = []
    
    if not historical_trades:
        # If no data, create some sample data for testing
        current_time = int(datetime.now().timestamp() * 1000)
        for i in range(10):
            timestamps.append(datetime.fromtimestamp((current_time - (10-i)*1000)/1000).strftime('%H:%M:%S'))
            prices.append(78000.0 + i*100)  # Sample price data
            quantities.append(0.1 + i*0.1)  # Sample quantity data
    else:
        for trade_json, _ in historical_trades:
            try:
                trade = json.loads(trade_json)
                timestamps.append(datetime.fromtimestamp(trade['timestamp']/1000).strftime('%H:%M:%S'))
                prices.append(float(trade['price']))
                quantities.append(float(trade['quantity']))
            except Exception as e:
                print(f"Error processing trade data: {e}")
    
    context = {
        'timestamps': json.dumps(timestamps),
        'prices': json.dumps(prices),
        'quantities': json.dumps(quantities),
        'data_count': len(timestamps),
        'now': datetime.now(),
    }
    
    return render(request, 'trading_data/time_series.html', context)
