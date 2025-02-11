# -*— coding:utf-8 -*-

"""
OKEx Future Market Server.
https://www.okex.com/docs/zh/#futures_ws-all

Author: HuangTao
Date:   2018/12/20
Email:  huangtao@ifclover.com
"""

import zlib
import json
import copy
import asyncio
import datetime

from quant import const
from quant.utils import tools
from quant.utils import logger
from quant.tasks import LoopRunTask
from quant.utils.web import Websocket
from quant.event import EventOrderbook, EventKline, EventTrade
from quant.order import ORDER_ACTION_BUY, ORDER_ACTION_SELL
from quant.platform.okex_future import OKExFutureRestAPI
from quant.heartbeat import heartbeat

def get_timestamp(dt=''):
    if not dt:
        dt = datetime.datetime.now()
    t = dt.isoformat()
    return t + "Z"

class OKExFuture:
    """ OKEx Future Market Server.

    Attributes:
        kwargs:
            platform: Exchange platform name, must be `okex_future`.
            wss: Exchange Websocket host address, default is "wss://real.okex.com:10442".
            symbols: symbol list, OKEx Future instrument_id list.
            channels: channel list, only `orderbook` , `kline` and `trade` to be enabled.
            orderbook_length: The length of orderbook's data to be published via OrderbookEvent, default is 10.
    """

    def __init__(self, **kwargs):
        self._platform = kwargs["platform"]
        self._wss = kwargs.get("wss", "wss://real.okex.com:10442")
        self._host = kwargs.get("host", "https://www.okex.com")
        self._symbols = list(set(kwargs.get("symbols")))
        self._channels = kwargs.get("channels")
        self._orderbook_length = kwargs.get("orderbook_length", 10)
        self._access_key = kwargs.get("access_key")
        self._secret_key = kwargs.get("secret_key")
        self._passphrase = kwargs.get("passphrase")

        self._orderbooks = {}  # orderbook data, e.g. {"symbol": {"bids": {"price": quantity, ...}, "asks": {...}}}

        url = self._wss + "/ws/v3"
        self._ws = Websocket(url, connected_callback=self.connected_callback,
                             process_binary_callback=self.process_binary)
        self._ws.initialize()
        LoopRunTask.register(self.send_heartbeat_msg, 5)

        self._rest_api = OKExFutureRestAPI(self._host, self._access_key, self._secret_key, self._passphrase)

        self._initialize()

    async def connected_callback(self):
        """After create connection to Websocket server successfully, we will subscribe orderbook/kline/trade event."""
        ches = []
        for ch in self._channels:
            if ch == "orderbook":
                for symbol in self._symbols:
                    ch = "futures/depth:{s}".format(s=symbol)
                    ches.append(ch)
            elif ch == "trade":
                for symbol in self._symbols:
                    ch = "futures/trade:{s}".format(s=symbol.replace("/", '-'))
                    ches.append(ch)
            elif ch == "kline":
                for symbol in self._symbols:
                    ch = "futures/candle60s:{s}".format(s=symbol.replace("/", '-'))
                    ches.append(ch)
            # else:
            #     logger.error("channel error! channel:", ch, caller=self)
            if len(ches) > 0:
                msg = {
                    "op": "subscribe",
                    "args": ches
                }
                await self._ws.send(msg)
                logger.info("subscribe orderbook/kline/trade success.", caller=self)

    async def send_heartbeat_msg(self, *args, **kwargs):
        data = "ping"
        if not self._ws:
            logger.error("Websocket connection not yeah!", caller=self)
            return
        await self._ws.send(data)

    async def process_binary(self, raw):
        """ Process message that received from Websocket connection.

        Args:
            raw: Raw binary message received from Websocket connection.
        """
        decompress = zlib.decompressobj(-zlib.MAX_WBITS)
        msg = decompress.decompress(raw)
        msg += decompress.flush()
        msg = msg.decode()
        if msg == "pong":  # Heartbeat message.
            return
        msg = json.loads(msg)
        # logger.debug("msg:", msg, caller=self)

        table = msg.get("table")
        if table == "futures/depth":
            if msg.get("action") == "partial":
                for d in msg["data"]:
                    await self.process_orderbook_partial(d)
            elif msg.get("action") == "update":
                for d in msg["data"]:
                    await self.process_orderbook_update(d)
        elif table == "futures/trade":
            for d in msg["data"]:
                await self.process_trade(d)
        elif table == "futures/candle60s":
            for d in msg["data"]:
                await self.process_kline(d)

    async def process_orderbook_partial(self, data):
        """Deal with orderbook partial message."""
        symbol = data.get("instrument_id")
        if symbol not in self._symbols:
            return
        asks = data.get("asks")
        bids = data.get("bids")
        self._orderbooks[symbol] = {"asks": {}, "bids": {}, "timestamp": 0}
        for ask in asks:
            price = float(ask[0])
            quantity = int(ask[1])
            self._orderbooks[symbol]["asks"][price] = quantity
        for bid in bids:
            price = float(bid[0])
            quantity = int(bid[1])
            self._orderbooks[symbol]["bids"][price] = quantity
        timestamp = tools.utctime_str_to_mts(data.get("timestamp"))
        self._orderbooks[symbol]["timestamp"] = timestamp

    async def process_orderbook_update(self, data):
        """Deal with orderbook update message."""
        symbol = data.get("instrument_id")
        asks = data.get("asks")
        bids = data.get("bids")
        timestamp = tools.utctime_str_to_mts(data.get("timestamp"))

        if symbol not in self._orderbooks:
            return
        self._orderbooks[symbol]["timestamp"] = timestamp

        for ask in asks:
            price = float(ask[0])
            quantity = int(ask[1])
            if quantity == 0 and price in self._orderbooks[symbol]["asks"]:
                self._orderbooks[symbol]["asks"].pop(price)
            else:
                self._orderbooks[symbol]["asks"][price] = quantity

        for bid in bids:
            price = float(bid[0])
            quantity = int(bid[1])
            if quantity == 0 and price in self._orderbooks[symbol]["bids"]:
                self._orderbooks[symbol]["bids"].pop(price)
            else:
                self._orderbooks[symbol]["bids"][price] = quantity

        await self.publish_orderbook(symbol)

    async def publish_orderbook(self, symbol):
        """Publish orderbook message to EventCenter via OrderbookEvent."""
        ob = copy.copy(self._orderbooks[symbol])
        if not ob["asks"] or not ob["bids"]:
            logger.warn("symbol:", symbol, "asks:", ob["asks"], "bids:", ob["bids"], caller=self)
            return

        ask_keys = sorted(list(ob["asks"].keys()))
        bid_keys = sorted(list(ob["bids"].keys()), reverse=True)
        if ask_keys[0] <= bid_keys[0]:
            logger.warn("symbol:", symbol, "ask1:", ask_keys[0], "bid1:", bid_keys[0], caller=self)
            return

        asks = []
        for k in ask_keys[:self._orderbook_length]:
            price = "%.8f" % k
            quantity = str(ob["asks"].get(k))
            asks.append([price, quantity])

        bids = []
        for k in bid_keys[:self._orderbook_length]:
            price = "%.8f" % k
            quantity = str(ob["bids"].get(k))
            bids.append([price, quantity])

        orderbook = {
            "platform": self._platform,
            "symbol": symbol,
            "asks": asks,
            "bids": bids,
            "timestamp": ob["timestamp"]
        }
        EventOrderbook(**orderbook).publish()
        logger.info("symbol:", symbol, "orderbook:", orderbook, caller=self)

    async def process_trade(self, data):
        """Deal with trade data, and publish trade message to EventCenter via TradeEvent."""
        symbol = data["instrument_id"]
        if symbol not in self._symbols:
            return
        action = ORDER_ACTION_BUY if data["side"] == "buy" else ORDER_ACTION_SELL
        price = "%.8f" % float(data["price"])
        quantity = "%.8f" % float(data["qty"])
        timestamp = tools.utctime_str_to_mts(data["timestamp"])

        # Publish EventTrade.
        trade = {
            "platform": self._platform,
            "symbol": symbol,
            "action": action,
            "price": price,
            "quantity": quantity,
            "timestamp": timestamp
        }
        EventTrade(**trade).publish()
        logger.info("symbol:", symbol, "trade:", trade, caller=self)

    async def process_kline(self, data):
        """ Deal with 1min kline data, and publish kline message to EventCenter via KlineEvent.

        Args:
            data: Newest kline data.
        """
        symbol = data["instrument_id"]
        if symbol not in self._symbols:
            return
        timestamp = tools.utctime_str_to_mts(data["candle"][0])
        _open = "%.8f" % float(data["candle"][1])
        high = "%.8f" % float(data["candle"][2])
        low = "%.8f" % float(data["candle"][3])
        close = "%.8f" % float(data["candle"][4])
        volume = "%.8f" % float(data["candle"][5])

        # Publish EventKline
        kline = {
            "platform": self._platform,
            "symbol": symbol,
            "open": _open,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "timestamp": timestamp,
            "kline_type": const.MARKET_TYPE_KLINE
        }
        EventKline(**kline).publish()
        logger.info("symbol:", symbol, "kline:", kline, caller=self)

    def _initialize(self):
        """ Initialize."""
        for channel in self._channels:
            if channel == const.MARKET_TYPE_KLINE_5M:
                heartbeat.register(self.create_kline_tasks, 1, const.MARKET_TYPE_KLINE_5M)
            elif channel == const.MARKET_TYPE_KLINE_15M:
                heartbeat.register(self.create_kline_tasks, 1, const.MARKET_TYPE_KLINE_15M)
            elif channel == const.MARKET_TYPE_KLINE_1H:
                heartbeat.register(self.create_kline_tasks, 1, const.MARKET_TYPE_KLINE_1H)
            # else:
            #     logger.error("channel error! channel:", channel, caller=self)

    async def create_kline_tasks(self, kline_type, *args, **kwargs):
        """ Create some tasks to fetch kline information

        Args:
            kline_type: Type of line, kline or kline_5m or kline_15m

        NOTE: Because of REST API request limit, we only send one request pre minute.
        """
        for index, symbol in enumerate(self._symbols):
            asyncio.get_event_loop().call_later(index, self.delay_kline_update, symbol, kline_type)

    def delay_kline_update(self, symbol, kline_type):
        """ Do kline update.
        """
        asyncio.get_event_loop().create_task(self.do_kline_update(symbol, kline_type))

    async def do_kline_update(self, symbol, kline_type):
        if kline_type == const.MARKET_TYPE_KLINE_5M:
            range_type = 5 * 60
        elif kline_type == const.MARKET_TYPE_KLINE_15M:
            range_type = 15 * 60
        elif kline_type == const.MARKET_TYPE_KLINE_1H:
            range_type = 60 * 60
        else:
            return
        start = get_timestamp(datetime.datetime.now() - datetime.timedelta(days=5))
        end = get_timestamp()
        result, error = await self._rest_api.get_kline(symbol, start, end, range_type)

        if error:
            return

        open, hig, low, close = [], [], [], []
        if len(result):
            for item in result:
                open.append(item[1])
                hig.append(item[2])
                low.append(item[3])
                close.append(item[4])

        kline = {
            "platform": self._platform,
            "symbol": symbol,
            "open": open[::-1],#最近的数据在最后面
            "high": hig[::-1],
            "low": low[::-1],
            "close": close[::-1],
            "kline_type": kline_type
        }
        print(kline)
        EventKline(**kline).publish()
        logger.info("symbol:", symbol, "kline:", kline, caller=self)

