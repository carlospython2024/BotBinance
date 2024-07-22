import logging
import requests
import time
import typing
import collections
import smtplib
import pandas as pd
from urllib.parse import urlencode
# from datetime import datetime, timedelta

import hmac
import hashlib

import websocket
import json

import threading

from database import WorkspaceData
from models import *
from strategies import TechnicalStrategy, BreakoutStrategy

from binance.exceptions import BinanceAPIException, BinanceRequestException
from requests.exceptions import Timeout, ReadTimeout, ConnectTimeout, RetryError, HTTPError
from requests.exceptions import ConnectionError as CError
import socket
from urllib3.exceptions import NewConnectionError, MaxRetryError, ConnectionError

logger = logging.getLogger()
MY_EMAIL = "carlospython89@gmail.com"
PASSWORD = "obuloxvfowrbshrs"
ALPHAVANTAGE_API_KEY = "41KILZGW81L9LM0G"
FINNHUB_API_KEY = "cpdm3opr01qh24flcte0cpdm3opr01qh24flcteg"

class BinanceClient:
    def __init__(self, public_key: str, secret_key: str, testnet: bool, futures: bool):

        """
        https://binance-docs.github.io/apidocs/futures/en
        :param public_key:
        :param secret_key:
        :param testnet:
        :param futures: if False, the Client will be a Spot API Client
        """

        self.futures = futures
        if self.futures:
            self.platform = "binance_futures"
            if testnet:
                self._base_url = "https://testnet.binancefuture.com"
                self._wss_url = "wss://stream.binancefuture.com/ws"
            else:
                self._base_url = "https://fapi.binance.com"
                self._wss_url = "wss://fstream.binance.com/ws"
        else:
            self.platform = "binance_spot"
            if testnet:
                self._base_url = "https://testnet.binance.vision"
                self._wss_url = "wss://testnet.binance.vision/ws"
            else:
                self._base_url = "https://api.binance.com"
                self._wss_url = "wss://stream.binance.com:9443/ws"

        self._public_key = public_key
        self._secret_key = secret_key

        self._headers = {'X-MBX-APIKEY': self._public_key}

        self.contracts = self.get_contracts()
        self.balances = self.get_balances()

        self.prices = dict()
        self.strategies: typing.Dict[int, typing.Union[TechnicalStrategy, BreakoutStrategy]] = dict()

        self.logs = []

        self._ws_id = 1
        self.ws: websocket.WebSocketApp
        self.reconnect = True
        self.ws_connected = False
        self.ws_subscriptions = {"bookTicker": [], "aggTrade": []}
        self.db = WorkspaceData()

        t = threading.Thread(target=self._start_ws)
        t.start()

        logger.info("Binance Futures Client successfully initialized")

    def _add_log(self, msg: str):

        """
        Add a log to the list so that it can be picked by the update_ui() method of the root component.
        :param msg:
        :return:
        """

        logger.info("%s", msg)
        self.logs.append({"log": msg, "displayed": False})

    def _generate_signature(self, data: typing.Dict) -> str:

        """
        Generate a signature with the HMAC-256 algorithm.
        :param data: Dictionary of parameters to be converted to a query string
        :return:
        """

        return hmac.new(self._secret_key.encode(), urlencode(data).encode(), hashlib.sha256).hexdigest()

    def _make_request(self, method: str, endpoint: str, data: typing.Dict):

        """
        Wrapper that normalizes the requests to the REST API and error handling.
        :param method: GET, POST, DELETE
        :param endpoint: Includes the /api/v1 part
        :param data: Parameters of the request
        :return:
        """

        if method == "GET":
            try:
                response = requests.get(self._base_url + endpoint, params=data, headers=self._headers)
            except Exception as e:  # Takes into account any possible error, most likely network errors
                logger.error("Connection error while making %s request to %s: %s", method, endpoint, e)
                return None

        elif method == "POST":
            try:
                response = requests.post(self._base_url + endpoint, params=data, headers=self._headers)
            except Exception as e:
                message = f"Connection error while making {method} request to {endpoint}: {e}"
                self.send_email("Connection error", message)
                logger.error(message)
                return None

        elif method == "DELETE":
            try:
                response = requests.delete(self._base_url + endpoint, params=data, headers=self._headers)
            except Exception as e:
                logger.error("Connection error while making %s request to %s: %s", method, endpoint, e)
                return None
        else:
            raise ValueError()

        if response.status_code == 200:  # 200 is the response code of successful requests
            return response.json()
        else:
            logger.error("Error while making %s request to %s: %s (error code %s)",
                         method, endpoint, response.json(), response.status_code)
            return None

    def get_contracts(self) -> typing.Dict[str, Contract]:

        """
        Get a list of symbols/contracts on the exchange to be displayed in the OptionMenus of the interface.
        :return:
        """

        if self.futures:
            exchange_info = self._make_request("GET", "/fapi/v1/exchangeInfo", dict())
        else:
            exchange_info = self._make_request("GET", "/api/v3/exchangeInfo", dict())

        contracts = dict()

        if exchange_info is not None:
            for contract_data in exchange_info['symbols']:
                if contract_data['quoteAsset'] == "USDT":
                    contracts[contract_data['symbol']] = Contract(contract_data, self.platform)

        return collections.OrderedDict(sorted(contracts.items()))  # Sort keys of the dictionary alphabetically

    def get_historical_candles(self, contract: Contract, interval: str) -> typing.List[Candle]:

        """
        Get a list of the most recent candlesticks for a given symbol/contract and interval.
        :param contract:
        :param interval: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M
        :return:
        """

        data = dict()
        data['symbol'] = contract.symbol
        data['interval'] = interval
        data['limit'] = 1000  # The maximum number of candles is 1000 on Binance Spot

        if self.futures:
            raw_candles = self._make_request("GET", "/fapi/v1/klines", data)
        else:
            raw_candles = self._make_request("GET", "/api/v3/klines", data)

        candles = []

        if raw_candles is not None:
            for c in raw_candles:
                candles.append(Candle(c, interval, self.platform))

        return candles

    def get_bid_ask(self, contract: Contract) -> typing.Dict[str, float]:

        """
        Get a snapshot of the current bid and ask price for a symbol/contract, to be sure there is something
        to display in the Watchlist.
        :param contract:
        :return:
        """

        data = dict()
        data['symbol'] = contract.symbol

        if self.futures:
            ob_data = self._make_request("GET", "/fapi/v1/ticker/bookTicker", data)
        else:
            ob_data = self._make_request("GET", "/api/v3/ticker/bookTicker", data)

        if ob_data is not None:
            if contract.symbol not in self.prices:  # Add the symbol to the dictionary if needed
                self.prices[contract.symbol] = {'bid': float(ob_data['bidPrice']), 'ask': float(ob_data['askPrice'])}
            else:
                self.prices[contract.symbol]['bid'] = float(ob_data['bidPrice'])
                self.prices[contract.symbol]['ask'] = float(ob_data['askPrice'])

            return self.prices[contract.symbol]

    def get_balances(self) -> typing.Dict[str, Balance]:

        """
        Get the current balance of the account, the data is different between Spot and Futures
        :return:
        """

        data = dict()
        data['timestamp'] = int(time.time() * 1000)
        data['signature'] = self._generate_signature(data)

        balances = dict()

        if self.futures:
            account_data = self._make_request("GET", "/fapi/v2/account", data)
        else:
            account_data = self._make_request("GET", "/api/v3/account", data)

        if account_data is not None:
            if self.futures:
                for a in account_data['assets']:
                    balances[a['asset']] = Balance(a, self.platform)
            else:
                for a in account_data['balances']:
                    balances[a['asset']] = Balance(a, self.platform)

        return balances

    def place_order(self, contract: Contract, order_type: str, quantity: float, side: str, price=None,
                    tif=None) -> OrderStatus:

        """
        Place an order. Based on the order_type, the price and tif arguments are not required
        :param contract:
        :param order_type: LIMIT, MARKET, STOP, TAKE_PROFIT, LIQUIDATION
        :param quantity:
        :param side:
        :param price:
        :param tif:
        :return:
        """
        try:
            data = dict()
            data['symbol'] = contract.symbol
            data['side'] = side.upper()
            data['quantity'] = round(int(quantity / contract.lot_size) * contract.lot_size, 8)  # int() to round down
            data['type'] = order_type.upper()  # Makes sure the order type is in uppercase

            if price is not None:
                data['price'] = round(round(price / contract.tick_size) * contract.tick_size, 8)
                data['price'] = '%.*f' % (contract.price_decimals, data['price'])  # Avoids scientific notation

            if tif is not None:
                data['timeInForce'] = tif

            data['timestamp'] = int(time.time() * 1000)
            data['signature'] = self._generate_signature(data)

            leverage = 50

            if self.futures:
                if leverage > 1:
                    leverage = self.get_max_leverage(contract.symbol, leverage)

                    data_leverage = {
                        'symbol': contract.symbol,
                        'leverage': leverage,
                        'timestamp': int(time.time() * 1000),
                        'isolated': contract.symbol,
                    }
                    data_leverage['signature'] = self._generate_signature(data_leverage)
                    self._make_request("POST", "/fapi/v1/leverage", data_leverage)
                order_status = self._make_request("POST", "/fapi/v1/order", data)
            else:
                order_status = self._make_request("POST", "/api/v3/order", data)

            if order_status is not None:
                message = f"symbol: {data['symbol']} side: {data['side']} quantity: {data['quantity']}"
                self.send_email("Binance Order Filled", message)
                if not self.futures:
                    if order_status['status'] == "FILLED":
                        # winsound.Beep(frequency=1000, duration=500)
                        order_status['avgPrice'] = self._get_execution_price(contract, order_status['orderId'])
                    else:
                        order_status['avgPrice'] = 0

                order_status = OrderStatus(order_status, self.platform)

            return order_status
        except Exception as e:
            print(f"error en funcion place_order {e}")

    def cancel_order(self, contract: Contract, order_id: int) -> OrderStatus:

        data = dict()
        data['orderId'] = order_id
        data['symbol'] = contract.symbol

        data['timestamp'] = int(time.time() * 1000)
        data['signature'] = self._generate_signature(data)

        if self.futures:
            order_status = self._make_request("DELETE", "/fapi/v1/order", data)
        else:
            order_status = self._make_request("DELETE", "/api/v3/order", data)

        if order_status is not None:
            if not self.futures:
                # Get the average execution price based on the recent trades
                order_status['avgPrice'] = self._get_execution_price(contract, order_id)
            order_status = OrderStatus(order_status, self.platform)

        return order_status

    def _get_execution_price(self, contract: Contract, order_id: int) -> float:

        """
        For Binance Spot only, find the equivalent of the 'avgPrice' key on the futures side.
        The average price is the weighted sum of each trade price related to the order_id
        :param contract:
        :param order_id:
        :return:
        """

        data = dict()
        data['timestamp'] = int(time.time() * 1000)
        data['symbol'] = contract.symbol
        data['signature'] = self._generate_signature(data)

        trades = self._make_request("GET", "/api/v3/myTrades", data)

        avg_price = 0

        if trades is not None:

            executed_qty = 0
            for t in trades:
                if t['orderId'] == order_id:
                    executed_qty += float(t['qty'])

            for t in trades:
                if t['orderId'] == order_id:
                    fill_pct = float(t['qty']) / executed_qty
                    avg_price += (float(t['price']) * fill_pct)  # Weighted sum

        return round(round(avg_price / contract.tick_size) * contract.tick_size, 8)

    def get_order_status(self, contract: Contract, order_id: int) -> OrderStatus:

        data = dict()
        data['timestamp'] = int(time.time() * 1000)
        data['symbol'] = contract.symbol
        data['orderId'] = order_id
        data['signature'] = self._generate_signature(data)

        if self.futures:
            order_status = self._make_request("GET", "/fapi/v1/order", data)
        else:
            order_status = self._make_request("GET", "/api/v3/order", data)

        if order_status is not None:
            if not self.futures:
                if order_status['status'] == "FILLED":
                    # Get the average execution price based on the recent trades
                    order_status['avgPrice'] = self._get_execution_price(contract, order_id)
                else:
                    order_status['avgPrice'] = 0

            order_status = OrderStatus(order_status, self.platform)

        return order_status

    def _start_ws(self):

        """
        Infinite loop (thus has to run in a Thread) that reopens the websocket connection in case it drops
        :return:
        """

        self.ws = websocket.WebSocketApp(self._wss_url, on_open=self._on_open, on_close=self._on_close,
                                         on_error=self._on_error, on_message=self._on_message)

        try:
            while self.reconnect:  # Reconnect unless the interface is closed by the user
                self.ws.run_forever()  # Blocking method that ends only if the websocket connection drops
                time.sleep(2)
        except (BinanceAPIException, BinanceRequestException) as Binance_Error:
            logger.debug(f'Binance Error in run_forever() method {Binance_Error}')
        except (socket.gaierror, NewConnectionError, MaxRetryError, ConnectionError) as Socket_Error:
            logger.debug(f'Socket Error in run_forever() method ConnectionError {Socket_Error}')
        except (Timeout, ReadTimeout, ConnectTimeout, RetryError, HTTPError, CError) as Conn_Error:
            logger.debug(f'Connection in run_forever() method Error {Conn_Error}')
        except Exception as e:
            connection_error = f"Binance Spot error in run_forever() method: {e}"
            self.send_email("Binance Spot error", connection_error)
            logger.debug(connection_error)

    def _on_open(self, ws):
        logger.info("Binance connection opened")

        self.ws_connected = True

        # The aggTrade channel is subscribed to in the _switch_strategy() method of strategy_component.py

        for channel in ["bookTicker", "aggTrade"]:
            for symbol in self.ws_subscriptions[channel]:
                self.subscribe_channel([self.contracts[symbol]], channel, reconnection=True)

        if "BTCUSDT" not in self.ws_subscriptions["bookTicker"]:
            self.subscribe_channel([self.contracts["BTCUSDT"]], "bookTicker")

    def _on_close(self, ws, *args, **kwargs):

        """
        Callback method triggered when the connection drops
        :return:
        """

        logger.warning("Binance Websocket connection closed")
        self.ws_connected = False

    def _on_error(self, ws, msg: str):

        """
        Callback method triggered in case of error
        :param msg:
        :return:
        """
        self.send_email("Binance connection error", msg)
        logger.error("Binance connection error: %s", msg)

    def _on_message(self, ws, msg: str):

        """
        The websocket updates of the channels the program subscribed to will go through this callback method
        :param msg:
        :return:
        """

        trades_counter = 0
        data = json.loads(msg)

        if "u" in data and "A" in data:
            data['e'] = "bookTicker"  # For Binance Spot, to make the data structure uniform with Binance Futures
            # See the data structure difference here: https://binance-docs.github.io/apidocs/spot/en/#individual-symbol-book-ticker-streams

        if "e" in data:
            if data['e'] == "bookTicker":

                symbol = data['s']

                if symbol not in self.prices:
                    self.prices[symbol] = {'bid': float(data['b']), 'ask': float(data['a'])}
                else:
                    self.prices[symbol]['bid'] = float(data['b'])
                    self.prices[symbol]['ask'] = float(data['a'])

                # PNL Calculation
                try:
                    for b_index, strat in self.strategies.items():
                        if strat.contract.symbol == symbol:
                            trades_counter = 0
                            for trade in strat.trades:
                                if trade.status == "open" and trade.entry_price is not None:
                                    trades_counter = trades_counter + 1
                                    if trade.side == "long":
                                        trade.pnl = (self.prices[symbol]['bid'] - trade.entry_price) * trade.quantity
                                    elif trade.side == "short":
                                        trade.pnl = (trade.entry_price - self.prices[symbol]['ask']) * trade.quantity
                except RuntimeError as e:  # Handles the case  the dictionary is modified while loop through it
                    logger.error("Error while looping through the Binance strategies: %s", e)

            if data['e'] == "aggTrade":

                symbol = data['s']

                ongoing_symbol = []
                count = 0
                max_trades_allowed = 2
                for strategies in self.strategies.items():
                    if strategies[1].ongoing_position:
                        ongoing_symbol.append(strategies[1].contract.symbol)
                        count += 1

                for key, strat in self.strategies.items():
                    if strat.contract.symbol == symbol:
                        if count < max_trades_allowed:
                            res = strat.parse_trades(float(data['p']), float(data['q']), data['T'])  # Updates candlesticks
                            strat.check_trade(res)
                        elif symbol in ongoing_symbol:
                            res = strat.parse_trades(float(data['p']), float(data['q']), data['T'])  # Updates candlesticks
                            strat.check_trade(res)

    def subscribe_channel(self, contracts: typing.List[Contract], channel: str, reconnection=False):

        """
        Subscribe to updates on a specific topic for all the symbols.
        If your list is bigger than 300 symbols, the subscription will fail (observed on Binance Spot).
        :param contracts:
        :param channel: aggTrades, bookTicker...
        :param reconnection: Force to subscribe to a symbol even if it already in self.ws_subscriptions[symbol] list
        :return:
        """

        if len(contracts) > 100:
            logger.warning("Subscribing to more than 100 symbols will most likely fail. "
                           "Consider subscribing only when adding a symbol to your Watchlist or when starting a "
                           "strategy for a symbol.")

        data = dict()
        data['method'] = "SUBSCRIBE"
        data['params'] = []

        if len(contracts) == 0:
            data['params'].append(channel)
        else:
            for contract in contracts:
                if contract.symbol not in self.ws_subscriptions[channel] or reconnection:
                    data['params'].append(contract.symbol.lower() + "@" + channel)
                    if contract.symbol not in self.ws_subscriptions[channel]:
                        self.ws_subscriptions[channel].append(contract.symbol)

            if len(data['params']) == 0:
                return

        data['id'] = self._ws_id

        try:
            self.ws.send(json.dumps(data))  # Converts the JSON object (dictionary) to a JSON string
            # logger.info("Binance: subscribing to: %s", ','.join(data['params']))
            # print(f"Subscribing to: {','.join(data['params'])}")
        except Exception as e:
            logger.error("Websocket error while subscribing to @bookTicker and @aggTrade: %s", e)

        self._ws_id += 1

    def get_trade_size(self, contract: Contract, price: float, balance_pct: float):

        """
        Compute the trade size for the strategy module based on the percentage of the balance to use
        that was defined in the strategy component.
        :param contract:
        :param price: Used to convert the amount to invest into an amount to buy/sell
        :param balance_pct:
        :return:
        """

        logger.info("Getting Binance trade size...")

        balance = self.get_balances()

        if balance is not None:
            if contract.quote_asset in balance:  # On Binance Spot, the quote asset isn't necessarily USDT
                if self.futures:
                    balance = balance[contract.quote_asset].wallet_balance
                else:
                    balance = balance[contract.quote_asset].free
            else:
                return None
        else:
            return None

        # @todo validate margin to get the trade zise balances['USDT'].margin_balance

        trade_size = (balance * balance_pct / 100) / price

        trade_size = round(round(trade_size / contract.lot_size) * contract.lot_size, 8)  # Removes extra decimals

        logger.info("Binance current %s balance = %s, trade size = %s", contract.quote_asset, balance, trade_size)

        return trade_size

    def get_max_leverage(self, symbol, leverage):
        try:
            params = {
                "symbol": symbol,
                "timestamp": int(time.time() * 1000)
            }
            params['signature'] = self._generate_signature(params)
            exchange_info = self._make_request("GET", "/fapi/v1/leverageBracket", params)

            for item in exchange_info:
                if item['symbol'] == symbol:
                    max_leverage = item['brackets'][1]['initialLeverage']
                    print(f"El apalancamiento máximo para {symbol} es {max_leverage}x")
                    leverage = min(int(leverage), int(max_leverage))
                    break
            return int(leverage)
        except Exception as e:
            print(f"error function get_max_leverage {e}")

    def check_max_quantity(self, quantity_to_check, symbol_to_check):
        try:
            data = self._make_request("GET", "/fapi/v1/exchangeInfo", dict())
            for symbol_info in data['symbols']:
                if symbol_info['symbol'] == symbol_to_check:
                    # print(symbol_info['filters'])
                    max_quantity = float(symbol_info['filters'][1]['maxQty'])  # El índice 1 corresponde a LOT_SIZE
                    if quantity_to_check > max_quantity:
                        quantity_to_check = max_quantity
                    break
            return float(quantity_to_check)
        except Exception as e:
            print(f"error function check_max_quantity {e}")

    def get_open_positions(self):
        try:
            params = {
                "timestamp": int(time.time() * 1000)
            }
            params['signature'] = self._generate_signature(params)
            position_data = self._make_request("GET", "/fapi/v2/positionRisk", params)
            return position_data
        except Exception as e:
            print(f"error function check_max_quantity {e}")

    def get_volume_color(self, symbol_to_check, interval):
        try:
            params = {
                "symbol": symbol_to_check,
                "interval": interval,
                "limit": 2,
            }
            data_klines = self._make_request("GET", "/fapi/v1/klines", params)

            color_last_candle = "red" if float(data_klines[0][4]) < float(data_klines[0][1]) else "green"
            color_previous_candle = "red" if float(data_klines[1][4]) < float(data_klines[1][1]) else "green"

            return color_last_candle, color_previous_candle

        except Exception as e:
            print(f"error function get_volume {e}")

    def get_day_data(self, symbol: str):
        params = {
            "symbol": symbol,
            "interval": "1d",
            "limit": 120
        }
        klines = self._make_request("GET", "/fapi/v1/klines", params)
        # Convertir los datos a un DataFrame
        df = pd.DataFrame(klines, columns=[
            'Open Time', 'Open', 'High', 'Low', 'Close', 'Volume',
            'Close Time', 'Quote Asset Volume', 'Number of Trades',
            'Taker Buy Base Asset Volume', 'Taker Buy Quote Asset Volume', 'Ignore'
        ])

        # Convertir el tiempo de apertura a una fecha legible
        df['Open Time'] = pd.to_datetime(df['Open Time'], unit='ms')

        # Seleccionar solo las columnas necesarias
        df = df[['Open Time', 'Open', 'High', 'Low', 'Close', 'Volume']]

        # Cambiar el tipo de datos de las columnas a float
        df[['Open', 'High', 'Low', 'Close', 'Volume']] = df[['Open', 'High', 'Low', 'Close', 'Volume']].astype(float)

        return df

    def get_external_float_data(self, symbol: str):
        url = f'https://finnhub.io/api/v1/stock/metric?symbol={symbol}&metric=all&token={FINNHUB_API_KEY}'
        response = requests.get(url)
        data = response.json()
        if 'metric' in data and 'float' in data['metric']:
            return data['metric']['float']
        else:
            print("No se pudieron obtener los datos históricos de Finhub. Verifica el símbolo del activo y la clave de API.")
            return None

    # Función para obtener la dominancia del Bitcoin
    def obtener_dominancia_bitcoin(self):
        url = "https://api.coingecko.com/api/v3/global"
        response = requests.get(url)
        data = response.json()
        bitcoin_market_cap = data['data']['market_cap_percentage']['btc']
        return bitcoin_market_cap

    # Función para obtener el precio del Bitcoin
    def obtener_precio_bitcoin(self):
        data = self.get_day_data("BTCUSDT")
        precio_actual = data['Close'].iloc[-1]
        precio_anterior = data['Close'].iloc[-2]
        return precio_actual, precio_anterior

    # Lógica para determinar el comportamiento esperado de los altcoins
    def determinar_comportamiento_altcoins(self, dominancia_actual, dominancia_anterior, precio_actual, precio_anterior):
        dominancia_subiendo = dominancia_actual > dominancia_anterior
        precio_subiendo = precio_actual > precio_anterior
        precio_bajando = precio_actual < precio_anterior
        precio_lateral = precio_actual == precio_anterior

        message = (f"Precio actual: {precio_actual:.2f}\n"
                   f"Precio anterior: {precio_anterior:.2f}\n"
                   f"Dominancia actual: {dominancia_actual:.2f}\n"
                   f"Dominancia anterior: {dominancia_anterior:.2f}\n")

        if dominancia_subiendo:
            if precio_subiendo:
                return f"La dominancia del Bitcoin esta subiendo y el precio subiendo, se espera que el precio de los altcoins baje. \n{message}"
            elif precio_bajando:
                return f"La dominancia del Bitcoin esta subiendo y el precio bajando, se espera que el precio de los altcoins baje rápidamente.\n{message}"
            elif precio_lateral:
                return f"La dominancia del Bitcoin esta subiendo y el precio lateralizando, se espera que el precio de los altcoins se estanque.\n{message}"
        else:
            if precio_subiendo:
                return f"La dominancia del Bitcoin no sube y el precio aumenta, se espera que el precio de los altcoins suba rápidamente.\n{message}"
            elif precio_bajando:
                return f"La dominancia del Bitcoin no sube y el precio disminuye, se espera que el precio de los altcoins se estanque.\n{message}"
            elif precio_lateral:
                return f"La dominancia del Bitcoin no sube y el precio se estanca, se espera que el precio de los altcoins suba.\n{message}"

    def check_market(self):
        print("Dominance del Bitcoin:")
        dominancia_actual = self.obtener_dominancia_bitcoin()
        # self.db.save('btc_dominance', [dominancia_actual])

        dominancia_anterior = self.obtener_dominancia_bitcoin()  #@todo En una implementación real, deberías almacenar y comparar con valores históricos.
        precio_actual, precio_anterior = self.obtener_precio_bitcoin()
        self.obtener_indice_miedo_codicia()

        # Determinar y mostrar el comportamiento esperado de los altcoins
        comportamiento_altcoins = self.determinar_comportamiento_altcoins(dominancia_actual, dominancia_anterior,
                                                                     precio_actual, precio_anterior)
        print(comportamiento_altcoins)

    import requests

    def obtener_indice_miedo_codicia(self):
        url = 'https://api.alternative.me/fng/'
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            indice = data
            if indice:
                print("Índice de miedo y codicia:")
                for item in indice['data']:
                    print(f"Valor: {item['value']}, Sentimiento: {item['value_classification']}")
            else:
                print("No se pudo obtener el índice de miedo y codicia.")
        else:
            return None


    def send_email(self, subject, message):
        with smtplib.SMTP("smtp.gmail.com") as connection:
            connection.starttls()
            connection.login(user=MY_EMAIL, password=PASSWORD)
            connection.sendmail(
                from_addr=MY_EMAIL,
                to_addrs="candrelosolaz@gmail.com",
                msg=f"Subject: {subject}!\n\n{message}"
            )
