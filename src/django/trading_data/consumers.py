import json
import logging
import redis
from channels.generic.websocket import AsyncWebsocketConsumer
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync

logger = logging.getLogger(__name__)

# Redis connection parameters
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0

class TradeConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        self.room_group_name = 'trades'
        
        # Join room group
        await self.channel_layer.group_add(
            self.room_group_name,
            self.channel_name
        )
        
        await self.accept()
        
        # Send historical trades
        try:
            redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
            historical_trades = redis_client.zrange('historical_trades:BTCUSDT', 0, -1)
            
            for trade in historical_trades:
                try:
                    trade_data = json.loads(trade)
                    await self.send(text_data=json.dumps({
                        'type': 'trade_update',
                        'trade': trade_data
                    }))
                except json.JSONDecodeError as e:
                    logger.error(f"Error decoding trade data: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error sending historical trades: {e}")

    async def disconnect(self, close_code):
        # Leave room group
        try:
            await self.channel_layer.group_discard(
                self.room_group_name,
                self.channel_name
            )
        except Exception as e:
            logger.error(f"Error disconnecting: {e}")

    async def trade_update(self, event):
        try:
            await self.send(text_data=json.dumps({
                'type': 'trade_update',
                'trade': event['trade']
            }))
        except Exception as e:
            logger.error(f"Error sending trade update: {e}")

def send_trade_update(trade_data):
    try:
        channel_layer = get_channel_layer()
        async_to_sync(channel_layer.group_send)(
            'trades',
            {
                'type': 'trade_update',
                'trade': trade_data
            }
        )
    except Exception as e:
        logger.error(f"Error sending trade update: {e}") 