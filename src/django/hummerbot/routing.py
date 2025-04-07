xfrom django.urls import re_path
from trading_data.consumers import TradeConsumer

websocket_urlpatterns = [
    re_path(r'ws/trades/$', TradeConsumer.as_asgi()),
] 