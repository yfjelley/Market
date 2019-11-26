# -*— coding:utf-8 -*-

"""
Huobi Future Market Server.
https://github.com/huobiapi/API_Docs/wiki/WS_api_reference_Derivatives

Author: HuangTao
Date:   2019/02/25
Email:  huangtao@ifclover.com
"""

import gzip
import json

from quant.utils import logger
from quant.utils.web import Websocket
from quant.event import EventOrderbook


class HuobiFutureMarket:
    """ Huobi Future Market Server.

    Attributes:
        kwargs:
            platform: Exchange platform name, must be `huobi_future`.
            wss: Exchange Websocket host address, default is `wss://www.hbdm.com`.
            symbols: symbol list, Huobi Future contract code list.
            channels: channel list, only `orderbook` to be enabled.
            orderbook_length: The length of orderbook's data to be published via OrderbookEvent, default is 10.
    """

    def __init__(self, **kwargs):
        self._platform = kwargs["platform"]
        self._wss = kwargs.get("wss", "wss://www.hbdm.com")
        self._symbols = list(set(kwargs.get("symbols")))
        self._channels = kwargs.get("channels")
        self._orderbook_length = kwargs.get("orderbook_length", 10)

        self._c_to_s = {}  # {"channel": "symbol"}

        url = self._wss + "/ws"
        self._ws = Websocket(url, self.connected_callback, process_binary_callback=self.process_binary)
        self._ws.initialize()

    async def connected_callback(self):
        """ After create connection to Websocket server successfully, we will subscribe orderbook event.
        """
        for ch in self._channels:
            if ch == "orderbook":
                for symbol in self._symbols:
                    channel = self._symbol_to_channel(symbol, "depth")
                    if not channel:
                        continue
                    data = {
                        "sub": channel
                    }
                    await self._ws.send(data)
            else:
                logger.error("channel error! channel:", ch, caller=self)

    async def process_binary(self, msg):
        """ Process binary message that received from Websocket connection.

        Args:
            msg: Binary message.
        """
        data = json.loads(gzip.decompress(msg).decode())
        logger.debug("data:", json.dumps(data), caller=self)
        channel = data.get("ch")
        if not channel:
            if data.get("ping"):
                hb_msg = {"pong": data.get("ping")}
                await self._ws.send(hb_msg)
            return

        symbol = self._c_to_s[channel]
        if channel.find("depth") != -1:
            await self.process_orderbook(symbol, data)

    async def process_orderbook(self, symbol, data):
        """Process orderbook message and publish OrderbookEvent.

        Args:
            symbol: Contract code.
            data: Orderbook data received from Websocket connection.
        """
        tick = data.get("tick")
        asks = tick.get("asks")[:self._orderbook_length]
        bids = tick.get("bids")[:self._orderbook_length]
        timestamp = tick.get("ts")
        orderbook = {
            "platform": self._platform,
            "symbol": symbol,
            "asks": asks,
            "bids": bids,
            "timestamp": timestamp
        }
        EventOrderbook(**orderbook).publish()
        logger.info("symbol:", symbol, "orderbook:", orderbook, caller=self)

    def _symbol_to_channel(self, symbol, channel_type):
        if channel_type == "depth":
            channel = "market.{s}.depth.step6".format(s=symbol)
        else:
            logger.error("channel type error! channel type:", channel_type, calle=self)
            return None
        self._c_to_s[channel] = symbol
        return channel
