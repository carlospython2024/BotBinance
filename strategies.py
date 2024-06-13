import logging
from typing import *
import time
from threading import Timer
import smtplib
import pandas
import pandas as pd
import pandas_ta
import numpy as np
from winotify import Notification, audio
from models import *
from datetime import datetime, timezone
import pytz
import talib
import talib.abstract as ta
import statsmodels.api as sm
from scipy.stats import linregress


if TYPE_CHECKING:  # Import the connector class names only for typing purpose (the classes aren't actually imported)
    from connectors.bitmex import BitmexClient
    from connectors.binance import BinanceClient

logger = logging.getLogger()

# TF_EQUIV is used in parse_trades() to compare the last candle timestamp to the new trade timestamp
TF_EQUIV = {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "4h": 14400}
MY_EMAIL = "carlospython89@gmail.com"
PASSWORD = "obuloxvfowrbshrs"
BULLISH = "bullish"
STRONG_BULLISH = "strong_bullish"
BEARISH = "bearish"
STRONG_BEARISH = "strong_bearish"
ICON_LONG = "C:\\Users\\carla\\OneDrive\\Imágenes\\long2.jpeg"
ICON_SHORT = "C:\\Users\\carla\\OneDrive\\Imágenes\\short.jpeg"
# @TODO hacer dinamico el leverage segun la hora, despues de las 7pm 20, despues de las 6am 50
LEVERAGE = 50


class Strategy:
    def __init__(self, client: Union["BitmexClient", "BinanceClient"], contract: Contract, exchange: str,
                 timeframe: str, balance_pct: float, take_profit: float, stop_loss: float, strat_name):
        pd.options.mode.copy_on_write = True
        self.client = client

        self.contract = contract
        self.exchange = exchange
        self.tf = timeframe
        self.tf_equiv = TF_EQUIV[timeframe] * 1000
        self.balance_pct = balance_pct
        self.take_profit = take_profit
        self.stop_loss = stop_loss

        self.strat_name = strat_name

        self.ongoing_position = False

        self.candles: List[Candle] = []
        self.trades: List[Trade] = []
        self.logs = []

        self.asset = f"{self.exchange}{self.contract.symbol}"
        pd.options.display.float_format = '{:.2f}'.format
        self._leverage = self.client.get_max_leverage(self.contract.symbol, LEVERAGE) - 1 if LEVERAGE > 1 else 1
        self.verified = False
        self.stop_loss_long = 0
        self.stop_loss_short = 0
        self.take_profit_long = 0
        self.take_profit_short = 0
        self.disabled_asset = False

        self.position_quantity = 0
        self.open_trades = 0
        self.last_close = 0

    def _add_log(self, msg: str):
        logger.info("%s", msg)
        self.logs.append({"log": msg, "displayed": False})

    def parse_trades(self, price: float, size: float, timestamp: int) -> str:

        """
        Parse new trades coming in from the websocket and update the Candle list based on the timestamp.
        :param price: The trade price
        :param size: The trade size
        :param timestamp: Unix timestamp in milliseconds
        :return:
        """
        if not self.disabled_asset:
            timestamp_diff = int(time.time() * 1000) - timestamp
            if timestamp_diff >= 200000:
                print("%s %s: %s milliseconds of difference between the current time and the trade time",
                      self.exchange, self.contract.symbol, timestamp_diff)

            last_candle = self.candles[-1]

            # Same Candle
            if timestamp < last_candle.timestamp + self.tf_equiv:

                last_candle.close = price
                last_candle.volume += size

                if price > last_candle.high:
                    last_candle.high = price
                elif price < last_candle.low:
                    last_candle.low = price

                # Check Take profit / Stop loss
                for trade in self.trades:
                    if trade.status == "open" and trade.entry_price is not None:
                        self.open_trades = self.open_trades + 1
                        self._check_tp_sl(trade, timestamp, last_candle)
                    if trade.status == "close":
                        self.open_trades = self.open_trades - 1

                self.last_close = last_candle.close

                return "same_candle"

            # Missing Candle(s)
            elif timestamp >= last_candle.timestamp + 2 * self.tf_equiv:

                missing_candles = int((timestamp - last_candle.timestamp) / self.tf_equiv) - 1

                print("%s missing %s candles for %s %s (%s %s)", self.exchange, missing_candles, self.contract.symbol,
                      self.tf, timestamp, last_candle.timestamp)

                for missing in range(missing_candles):
                    new_ts = last_candle.timestamp + self.tf_equiv
                    candle_info = {'ts': new_ts, 'open': last_candle.close, 'high': last_candle.close,
                                   'low': last_candle.close, 'close': last_candle.close, 'volume': 0}
                    new_candle = Candle(candle_info, self.tf, "parse_trade")

                    self.candles.append(new_candle)

                    last_candle = new_candle

                new_ts = last_candle.timestamp + self.tf_equiv
                candle_info = {'ts': new_ts, 'open': price, 'high': price, 'low': price, 'close': price, 'volume': size}
                new_candle = Candle(candle_info, self.tf, "parse_trade")

                self.candles.append(new_candle)

                return "new_candle"

            # New Candle
            elif timestamp >= last_candle.timestamp + self.tf_equiv:
                new_ts = last_candle.timestamp + self.tf_equiv
                candle_info = {'ts': new_ts, 'open': price, 'high': price, 'low': price, 'close': price, 'volume': size}
                new_candle = Candle(candle_info, self.tf, "parse_trade")

                self.candles.append(new_candle)
                hour = datetime.now()
                formatted_time = hour.strftime("%Y-%m-%d %H:%M:%S,%f")
                print(f"{formatted_time} :: New candle for {self.contract.symbol} {self.tf}")

                return "new_candle"

    def _check_order_status(self, order_id):

        """
        Called regularly after an order has been placed, until it is filled.
        :param order_id: The order id to check.
        :return:
        """

        order_status = self.client.get_order_status(self.contract, order_id)

        if order_status is not None:

            logger.info("%s order status: %s", self.exchange, order_status.status)

            if order_status.status == "filled":
                for trade in self.trades:
                    if trade.entry_id == order_id:
                        trade.entry_price = order_status.avg_price
                        trade.quantity = order_status.executed_qty
                        break
                return

        t = Timer(2.0, lambda: self._check_order_status(order_id))
        t.start()

    def _open_position(self, signal_result: int):

        """
        Open Long or Short position based on the signal result.
        :param signal_result: 1 (Long) or -1 (Short)
        :return:
        """

        # # Short is not allowed on Spot platforms
        # if self.client.platform == "binance_spot" and signal_result == -1:
        #     return
        #
        try:
            order_side = None
            if signal_result == 1:
                order_side = "buy"
            elif signal_result == -1:
                order_side = "sell"

            position_side = "long" if signal_result == 1 else "short"

            trade_size = self.client.get_trade_size(self.contract, self.candles[-1].close, self.balance_pct)

            if trade_size is None:
                return

            self._add_log(f"{position_side.capitalize()} signal on {self.contract.symbol} {self.tf}")

            leverage = self._leverage

            quantity_to_check = trade_size * leverage
            quantity_checked = self.client.check_max_quantity(float(quantity_to_check), self.contract.symbol)
            trade_size = quantity_checked

            order_status = self.client.place_order(self.contract, "MARKET", trade_size, order_side)

            if order_status is not None:
                self._add_log(
                    f"{order_side.capitalize()} order placed on {self.exchange} | Status: {order_status.status}")

                self.ongoing_position = True

                avg_fill_price = None

                if order_status.status == "filled":

                    avg_fill_price = order_status.avg_price
                else:
                    t = Timer(2.0, lambda: self._check_order_status(order_status.order_id))
                    t.start()

                new_trade = Trade({"time": int(time.time() * 1000), "entry_price": avg_fill_price,
                                   "contract": self.contract, "strategy": self.strat_name, "side": position_side,
                                   "status": "open", "pnl": 0, "quantity": order_status.executed_qty,
                                   "entry_id": order_status.order_id})
                self.trades.append(new_trade)
        except Exception as e:
            print(f"function _open_position {e}")

    def get_target_price_up(self, init_price, percentage):
        return init_price * (1 + percentage / 100)

    def get_target_price_down(self, init_price, percentage):
        return init_price * (1 + percentage / 100)

    def _check_tp_sl(self, trade: Trade, timestamp, last_candle):
        """
        Based on the average entry price, calculates whether the defined stop loss or take profit has been reached.
        :param trade:
        :return:
        """
        price = self.candles[-1].close
        tp_triggered = False
        sl_triggered = False
        # self.strat_name == "Breakout"
        closes = []
        opens = []
        lows = []
        highs = []
        volume = []
        for candle in self.candles:
            closes.append(candle.close)
            opens.append(candle.open)
            lows.append(candle.low)
            highs.append(candle.high)
            volume.append(candle.volume)

        close_arr = np.array(closes)
        low_arr = np.array(lows)
        high_arr = np.array(highs)
        volume_arr = np.array(volume)
        data = pd.DataFrame(
            {'Close': close_arr, 'Low': low_arr, 'High': high_arr, 'Volume': volume_arr})

        vwap_asc, vwap_desc, _ = self._vwap_reg(data, 11, 10)
        if trade.side == "long":
            if price >= trade.entry_price * (1 + self.take_profit / 100) and vwap_desc:
                tp_triggered = True
            if price <= trade.entry_price * (1 - self.stop_loss / 100):
                sl_triggered = True
        elif trade.side == "short":
            if price <= trade.entry_price * (1 - self.take_profit / 100) and vwap_asc:
                tp_triggered = True
            if price >= trade.entry_price * (1 + self.stop_loss / 100):
                sl_triggered = True

        if tp_triggered or sl_triggered:
            message = f"{'Stop loss' if sl_triggered else 'Take profit'} for {self.contract.symbol} {self.tf} " \
                      f"| Current Price = {price} (Entry price was {trade.entry_price}), " \
                      f"| Trade side: {trade.side}"
            self._add_log(message)
            order_side = "SELL" if trade.side == "long" else "BUY"

            trade.quantity = trade.quantity if self.position_quantity == 0 else self.position_quantity
            order_status = self.client.place_order(self.contract, "MARKET", trade.quantity, order_side)
            if order_status is not None:
                self._add_log(f"Exit order on {self.contract.symbol} {self.tf} placed successfully")
                trade.status = "closed"
                self.ongoing_position = False


    def create_data_frame(self):
        candles = self.candles
        closes = []
        opens = []
        lows = []
        highs = []
        volume = []
        timestamps = []
        for candle in candles:
            closes.append(candle.close)
            opens.append(candle.open)
            lows.append(candle.low)
            highs.append(candle.high)
            volume.append(candle.volume)
            timestamps.append(candle.timestamp)

        timestamps_datetime = [datetime.fromtimestamp(timestamp / 1000) for timestamp in timestamps]
        formatted_timestamps = [timestamp.strftime("%Y-%m-%d %H:%M:%S") for timestamp in timestamps_datetime]

        close_arr = np.array(closes)
        open_arr = np.array(opens)
        low_arr = np.array(lows)
        high_arr = np.array(highs)
        volume_arr = np.array(volume)
        timestamp_arr = np.array(formatted_timestamps)
        data = pd.DataFrame(
            {'Open': open_arr, 'Close': close_arr, 'Low': low_arr, 'High': high_arr, 'Volume': volume_arr, 'Datetime': timestamp_arr})

        return data

    def calculate_relative_volume(self, df, period=20):
        df['Avg_Volume'] = df['Volume'].rolling(window=period).mean()
        df['Relative_Volume'] = df['Volume'] / df['Avg_Volume']
        return df

    def calculate_gap_up_percentage(self):
        self.verified = True
        symbol = self.contract.symbol

        df = self.client.get_day_data(symbol)
        # High Gap Up
        df['Prev_Close'] = df['Close'].shift(1)
        df['Gap_Up_Pct'] = ((df['Open'] - df['Prev_Close']) / df['Prev_Close']) * 100

        # Relative Volume
        df['Avg_Volume'] = df['Volume'].rolling(window=20).mean()
        df['Relative_Volume'] = df['Volume'] / df['Avg_Volume']

        # print(f"The current gap up percentage for {self.contract.symbol} is {df['Gap_Up_Pct'].iloc[-1]} Relative Volume is {df['Relative_Volume'].iloc[-1]}")
        if df['Gap_Up_Pct'].iloc[-1] < 0 and df['Relative_Volume'].iloc[-1] >= 0.6:
            print(f"It's likely that today will mark the beginning of the {self.contract.symbol} BEAR trend Relative Volume is {df['Relative_Volume'].iloc[-1]} and GAP Up Percentage {df['Gap_Up_Pct'].iloc[-1]}.")
        elif df['Gap_Up_Pct'].iloc[-1] > 0 and df['Relative_Volume'].iloc[-1] >= 0.6:
            print(f"It's likely that today will mark the beginning of the {self.contract.symbol} BULL trend Relative Volume is {df['Relative_Volume'].iloc[-1]} and GAP Up Percentage {df['Gap_Up_Pct'].iloc[-1]}.")
        elif df['Relative_Volume'].iloc[-1] >= 3 and df['Gap_Up_Pct'].iloc[-1] == 0:
            print(f"There no High Gap today, but Relative volume is high {df['Relative_Volume'].iloc[-1]} for {self.contract.symbol}.")
        elif df['Relative_Volume'].iloc[-1] >= 0.6:
            print(f"There is not Gap Up right now for {self.contract.symbol} and the Relative Volume is {df['Relative_Volume'].iloc[-1]}.")
        elif df['Gap_Up_Pct'].iloc[-1] != 0:
            print(f"There is a Gap Up of {df['Gap_Up_Pct'].iloc[-1]}%, but the Volume is low for {self.contract.symbol} Relative Volume is {df['Relative_Volume'].iloc[-1]}.")
        else:
            print(f"Don't trade today with {self.contract.symbol} GAP Up Percentage {df['Gap_Up_Pct'].iloc[-1]} Relative Volume {df['Relative_Volume'].iloc[-1]}  this Asset is disabled.")
            self.disabled_asset = True

    def _ut_bot_alerts(
            self, ut_key_value_parent: float,
            ut_atr_period_parent: int,
            heikin_ashi: bool,
            close: np.ndarray,
            high: np.ndarray,
            low: np.ndarray,
            open: list):
        try:
            # Calcular ATR y trailing stop
            # Datos de ejemplo

            # Crear un DataFrame a partir de las listas
            a = ut_key_value_parent
            c = ut_atr_period_parent

            # Calcular ATR usando TA-Lib
            xATR = talib.ATR(high, low, close, c)

            # Calcular nLoss
            nLoss = a * xATR

            if heikin_ashi:
                ha_close = []
                ha_close.append((open[0] + high[0] + low[0] + close[0]) / 4)
                for i in range(1, len(open)):
                    ha_close.append((open[i] + high[i] + low[i] + close[i]) / 4)
                close = ha_close

            # Calcular xATRTrailingStop
            xATRTrailingStop = np.zeros_like(close)
            for i in range(1, len(close)):
                prev_stop = xATRTrailingStop[i - 1] if xATRTrailingStop[i - 1] != 0 else 0
                if close[i] > prev_stop and close[i - 1] > prev_stop:
                    xATRTrailingStop[i] = max(prev_stop, close[i] - nLoss[i])
                elif close[i] < prev_stop and close[i - 1] < prev_stop:
                    xATRTrailingStop[i] = min(prev_stop, close[i] + nLoss[i])
                elif close[i] > prev_stop:
                    xATRTrailingStop[i] = close[i] - nLoss[i]
                else:
                    xATRTrailingStop[i] = close[i] + nLoss[i]

            # Calcular EMA usando TA-Lib
            ema = np.array(close)

            # Calcular señales de compra y venta
            above = np.zeros_like(close, dtype=bool)
            below = np.zeros_like(close, dtype=bool)
            buy = np.zeros_like(close, dtype=bool)
            sell = np.zeros_like(close, dtype=bool)

            for i in range(1, len(close)):
                above[i] = ema[i] > xATRTrailingStop[i - 1] >= ema[i - 1]
                below[i] = ema[i] < xATRTrailingStop[i - 1] <= ema[i - 1]
                buy[i] = close[i] > xATRTrailingStop[i - 1] and above[i]
                sell[i] = close[i] < xATRTrailingStop[i - 1] and below[i]

            last_true_buy = len(buy) - np.flip(buy).argmax() - 1 if np.any(buy) else -1
            last_true_sell = len(sell) - np.flip(sell).argmax() - 1 if np.any(sell) else -1

            last_ut_signal = None

            if last_true_buy > last_true_sell:
                last_ut_signal = "buy"
            elif last_true_buy < last_true_sell:
                last_ut_signal = "sell"

            return buy[-1], sell[-1], close[-2], last_ut_signal

        except Exception as e:
            print(f"error function _ut_bot_alerts {e}")

    def heikin_ashi(self, open, high, low, close):
        ha_open = []
        ha_high = []
        ha_low = []
        ha_close = []

        ha_open.append((open[0] + close[0]) / 2)
        ha_high.append(high[0])
        ha_low.append(low[0])
        ha_close.append((open[0] + high[0] + low[0] + close[0]) / 4)

        for i in range(1, len(open)):
            ha_open.append((ha_open[i - 1] + ha_close[i - 1]) / 2)
            ha_high.append(max(high[i], open[i], close[i]))
            ha_low.append(min(low[i], open[i], close[i]))
            ha_close.append((open[i] + high[i] + low[i] + close[i]) / 4)

        return ha_open, ha_high, ha_low, ha_close

    def get_candle_type(self, candle_open: float, candle_high: float, candle_low: float, candle_close: float):
        if (candle_high - candle_close) > (candle_open - candle_low):
            return BULLISH
        elif (candle_close - candle_low) > (candle_high - candle_open):
            return BEARISH

    def heikin_ashi_strat(self, open_n: list, high: list, low: list, close: list, slope: int):
        ha_open, ha_high, ha_low, ha_close = self.heikin_ashi(open_n, high, low, close)
        ema_10 = self.ema(close, 10)
        ema_30 = self.ema(close, 30)

        last_heikin_ashi_candle_type = self.get_candle_type(ha_open[-2], ha_high[-2], ha_low[-2], ha_close[-2])
        buy_heikin = False
        sell_heikin = False

        rsi, rsi_ma = self._rsi_ma(close)

        if last_heikin_ashi_candle_type:
            if (slope > 0 and ha_close[-2] > ema_10[-1] > ema_30[-1] < ha_close[-1] and ha_open[-3] < ema_10[-3] and
                    last_heikin_ashi_candle_type == BULLISH and 50 > rsi > rsi_ma):
                buy_heikin = True
            if (slope < 0 and ha_close[-2] < ema_10[-1] < ema_30[-1] > ha_open[-1] and ha_close[-3] > ema_10[-3] and
                    last_heikin_ashi_candle_type == BEARISH and 50 < rsi < rsi_ma):
                sell_heikin = True

        return buy_heikin, sell_heikin, last_heikin_ashi_candle_type

    def atr(self, df, period):
        high_low = df['high'] - df['low']
        high_close = abs(df['high'] - df['close'].shift())
        low_close = abs(df['low'] - df['close'].shift())
        tr = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = np.max(tr, axis=1)
        atr = true_range.rolling(window=period).mean()
        return atr

    def change(self, source):
        """
        Computes the change of the source value over a specified length of bars.

        Args:
        source (list, array-like): The source values (e.g., closing prices).

        Returns:
        pandas.Series: Series containing the changes.
        """
        changes = [source[i] - source[i - 1] if i > 0 else None for i in range(len(source))]
        return pd.Series(changes, index=range(len(changes)))

    def ema(self, source: list, length: int):
        """
        Calculates the Exponential Moving Average (EMA) of a given source.

        Args:
        source (list, array-like): The input data for which EMA is to be calculated.
        length (int): The length of the EMA window.

        Returns:
        np.ndarray: Series containing the Exponential Moving Average.
        """
        if len(source) < length:
            raise ValueError("Length of source data must be greater than or equal to the specified length")

        close_prices = np.array(source)

        return talib.EMA(close_prices, timeperiod=length)

    def ema2(self, source, length):
        """
        Calcula la media móvil exponencial (EMA) de una serie de datos.

        Parámetros:
        source (list o numpy.ndarray): Serie de datos de entrada.
        length (int): Período de la EMA.

        Retorna:
        numpy.ndarray: Serie de valores de la EMA.
        """
        # Convertir la fuente a un array de NumPy
        source = np.array(source)

        # Calcular el factor de suavizado
        alpha = 2 / (length + 1)

        # Inicializar la EMA con el primer valor de la fuente
        ema_values = [source[0]]

        # Calcular la EMA para el resto de valores
        for i in range(1, len(source)):
            ema_values.append(alpha * source[i] + (1 - alpha) * ema_values[-1])

        return np.array(ema_values)

    def stdev(self, source, length, biased=True):
        """
        Calculates the standard deviation of a series of values.

        Args:
        source (list, array-like): The input data for which standard deviation is to be calculated.
        length (int): The number of data points to consider for the calculation.
        biased (bool, optional): Determines whether to use the biased or unbiased estimation of standard deviation.
                                  Defaults to True (biased).

        Returns:
        pandas.Series: Series containing the standard deviation values.
        """
        if len(source) < length:
            raise ValueError("Length of source data must be greater than or equal to the specified length")

        # Calculate the standard deviation
        if biased:
            stdev_values = [np.std(source[i - length + 1:i + 1]) if i >= length - 1 else None for i in
                            range(len(source))]
        else:
            stdev_values = [np.std(source[i - length + 1:i + 1], ddof=1) if i >= length - 1 else None for i in
                            range(len(source))]

        return pd.Series(stdev_values, index=range(len(stdev_values)))

    def get_ema(self, df, period):
        return df['close'].ewm(span=period, adjust=False).mean()

    def ut_bot_alert_2(self, ut_key_value, atr_period, closes, highs, lows):
        a = ut_key_value
        c = atr_period
        close_arr = np.array(closes)
        low_arr = np.array(lows)
        high_arr = np.array(highs)
        df = pd.DataFrame({'Close': close_arr, 'Low': low_arr, 'High': high_arr})

        df['xATR'] = talib.ATR(df["High"], df["Low"], df["Close"], c)
        df['nLoss'] = a * df['xATR']

        # Initialize variables
        df['xATRTrailingStop'] = 0.0
        df['pos'] = 0

        # Calculate xATRTrailingStop and position
        for i in range(1, len(df)):
            if df['close'][i - 1] < df['xATRTrailingStop'][i - 1] and df['close'][i] > df['xATRTrailingStop'][i - 1]:
                df.loc[i, 'pos'] = 1
                df.loc[i, 'xATRTrailingStop'] = max(df['xATRTrailingStop'][i - 1], df['close'][i] - df['nLoss'][i])
            elif df['close'][i - 1] > df['xATRTrailingStop'][i - 1] and df['close'][i] < df['xATRTrailingStop'][i - 1]:
                df.loc[i, 'pos'] = -1
                df.loc[i, 'xATRTrailingStop'] = min(df['xATRTrailingStop'][i - 1], df['close'][i] + df['nLoss'][i])
            else:
                df.loc[i, 'pos'] = df['pos'][i - 1]
                if df['close'][i - 1] > df['xATRTrailingStop'][i - 1]:
                    df.loc[i, 'xATRTrailingStop'] = df['close'][i] - df['nLoss'][i]
                else:
                    df.loc[i, 'xATRTrailingStop'] = df['close'][i] + df['nLoss'][i]

        # Calculate EMA
        df['ema'] = talib.EMA(np.array(closes), timeperiod=1)

        # Identify buy and sell signals
        df['above'] = df['ema'] > df['xATRTrailingStop']
        df['below'] = df['xATRTrailingStop'] > df['ema']
        # df['buy'] = (df['close'] > df['xATRTrailingStop']) & df['above']
        # df['sell'] = (df['close'] < df['xATRTrailingStop']) & df['below']
        green_candles = (df['close'] > df['xATRTrailingStop']) & df['above']
        red_candles = (df['close'] < df['xATRTrailingStop']) & df['below']

        return green_candles.values[-2], red_candles.values[-2]

    def linear_candle_color(self, closes, opens):
        signal_length = 7
        linreg_length = 11
        opens_prices = pd.Series(opens)
        close_prices = pd.Series(closes)
        b_open = self.linear_reg_series(opens_prices, linreg_length, 0)
        b_close = self.linear_reg_series(close_prices, linreg_length, 0)

        last_candle_green = False
        last_candle_red = False
        if b_close.values[-2] > b_open.values[-2]:
            last_candle_green = True
        elif b_close.values[-2] < b_open.values[-2]:
            last_candle_red = True

        return last_candle_green, last_candle_red

    def linear_reg_series(self, source: pandas.Series, length: int, offset: int):

        if len(source) < length:
            raise ValueError("La longitud de la serie es menor que el tamaño de la ventana")

            # Calcular los índices para la serie ajustada
        start_index = len(source) - length - offset
        end_index = len(source) - offset

        # Obtener los datos de la serie ajustada
        adjusted_data = source[start_index:end_index]

        # Calcular los valores necesarios para la regresión lineal
        x_values = np.arange(length)
        y_values = np.array(adjusted_data)
        x_mean = np.mean(x_values)
        y_mean = np.mean(y_values)

        # Calcular la pendiente (slope) y la intersección (intercept) de la recta de regresión
        numerator = np.sum((x_values - x_mean) * (y_values - y_mean))
        denominator = np.sum((x_values - x_mean) ** 2)
        slope = numerator / denominator
        intercept = y_mean - slope * x_mean

        # Calcular el valor de la regresión lineal para cada punto de la serie
        linreg_values = intercept + slope * (x_values)

        return linreg_values

    def sma(self, source, length):
        """
        Calculate the Simple Moving Average (SMA).

        Parameters:
        - source: List or numpy array of values.
        - length: Integer, number of values to consider for the average.

        Returns:
        - Numpy array of the Simple Moving Average.
        """
        if len(source) < length:
            raise ValueError("Insufficient data for the specified length.")

        sma_values = np.convolve(source, np.ones(length) / length, mode='valid')
        return np.concatenate([np.full(length - 1, fill_value=np.nan), sma_values])

    def linear_regression_signal(self, closes):
        signal_length = 16
        # linreg_length = 11
        # close_prices = np.array(closes)

        # Calcular la regresión lineal
        # b_close = talib.LINEARREG(close_prices, timeperiod=linreg_length)

        # Calcular la media móvil simple (SMA) del resultado de la regresión lineal
        signal = talib.SMA(closes, timeperiod=signal_length)

        # last_candle_green = False
        # last_candle_red = False
        # if b_close[-3] > b_open[-3] and b_close[-2] > b_open[-2] and signal[-2] < signal[-1]:
        #     last_candle_green = True
        # elif b_close[-3] < b_open[-3] and b_close[-2] < b_open[-2] and signal[-2] > signal[-1]:
        #     last_candle_red = True

        return signal[-2]

    def display_linear_regression_signal(self, closes, opens, linreg_length):
        opens_prices = pd.Series(opens)
        close_prices = pd.Series(closes)
        b_open = self.linear_reg_series(opens_prices, linreg_length, 0)
        b_close = self.linear_reg_series(close_prices, linreg_length, 0)
        last_candle_green = False
        last_candle_red = False
        if b_close[-1] > b_open[-1]:
            last_candle_green = True
            print(f"last_candle_green {self.contract.symbol}")
        else:
            last_candle_red = True
            print(f"last_candle_red {self.contract.symbol}")
        return last_candle_green, last_candle_red

    def linear_reg_value(self, source, length, offset):
        try:
            """
            Calculate the linear regression curve using the least squares method.

            Parameters:
            - source: Series of prices.
            - length: User-defined time period.
            - offset: Offset value.

            Returns:
            - Series[float]: Linear regression curve.
            """
            # Ensure the length is valid
            if length <= 1 or length > len(source):
                raise ValueError("Invalid length parameter")

            # Calculate the independent variable (x) for the linear regression
            x = np.arange(length)

            # Calculate the dependent variable (y) using the specified offset
            y = source.iloc[-length - offset:-offset] if offset > 0 else source.iloc[-length:]

            # Add a constant term to the independent variable for the intercept
            X = sm.add_constant(x)

            # Fit the linear regression model
            model = sm.OLS(y, X).fit()

            # Get the intercept and slope
            intercept, slope = model.params.values[0], model.params.values[1]
            # Calculate the linear regression curve using the formula
            linreg_values = intercept + slope * (length - 1 - offset)

            return linreg_values
        except Exception as e:
            print(f"error function linreg2 {e}")

    def slope_ling_reg(self, closes, len_r):
        data = pd.Series(closes)
        lin1 = self.linear_reg_value(data, len_r, 0)
        lin2 = self.linear_reg_value(data, len_r, 1)
        slope = lin1 - lin2
        trend = BULLISH if slope > 0 else BEARISH if slope < 0 else None

        return trend

    def linear_regression_channel(self, closes, length):
        """
        Calculate the endpoints of the linear regression channel.

        Parameters:
        - source: Series of prices.
        - length: User-defined time period.

        Returns:
        - tuple: (intercept, endy, slope) of the linear regression channel.
        """
        source = pd.Series(closes)
        # Ensure the length is valid
        if length <= 1 or length > len(source):
            raise ValueError("Invalid length parameter")

        # Calculate the midpoint of the channel
        # mid = np.sum(source[-length:]) / length

        # Calculate the slope using linear regression
        if -3 < closes[-1] < 20:
            closes_copy = [x * 1000 for x in closes]
            source2 = pd.Series(closes_copy)
            slope = self.linear_reg_value(source2, length, 0) - self.linear_reg_value(source2, length, 1)
        else:
            slope = self.linear_reg_value(source, length, 0) - self.linear_reg_value(source, length, 1)

        trend = BULLISH if slope > 0 else BEARISH if slope < 0 else None

        return trend, slope

    def get_slope(self, closes: np.ndarray, len_val: int):
        prices = closes

        # Cálculo de la regresión lineal
        x = np.arange(len(prices))
        slope, _ = np.polyfit(x[-len_val:], prices[-len_val:], 1)

        # # Determinar la dirección del movimiento
        if slope > 0:
            trend = STRONG_BULLISH if slope > np.polyfit(x[-len_val - 1:-1], prices[-len_val - 1:-1], 1)[0] else BULLISH
        elif slope < 0:
            trend = STRONG_BEARISH if slope < np.polyfit(x[-len_val - 1:-1], prices[-len_val - 1:-1], 1)[0] else BEARISH
        else:
            trend = None
        split = str(slope).split('.')
        slope_int = int(split[0])

        return slope_int, slope, trend

    def stoch(self, close, high, low, length):
        """
        Stochastic. It is calculated by a formula:
        100 * (close - lowest(low, length)) / (highest(high, length) - lowest(low, length)).

        Parameters:
            close (list or numpy array): Close prices.
            high (list or numpy array): High prices.
            low (list or numpy array): Low prices.
            length (int): Length of the period.

        Returns:
            numpy array: Stochastic values.
        """

        # Calculate the lowest low and highest high for the given length
        lowest_low = np.min(low[-length:])
        highest_high = np.max(high[-length:])

        # Calculate stochastic
        stoch_val = 100 * (close - lowest_low) / (highest_high - lowest_low)

        return stoch_val

    def get_stochastic2(self, closes, lows, highs):
        # Stochastic Slow
        period_k = 14
        smooth_d = 3

        stochastic = self.stoch(closes, highs, lows, period_k)
        k = self.sma(stochastic, 4)
        d = self.sma(k, smooth_d)
        return k[-1], d[-1]

    def get_stochastic_rsi(self, closes, lows, highs):
        length_k = 14
        smooth_k = 3
        smooth_d = 3
        stoch_rsi, stoch_rsi_signal = talib.STOCHRSI(np.array(closes), timeperiod=length_k, fastk_period=smooth_k,
                                                     fastd_period=smooth_d)

        return stoch_rsi[-1], stoch_rsi_signal[-1]

    def relative_volatility_index(self, src):
        length = 10
        len_param = 14

        # Calcular la desviación estándar
        stddev = self.stdev(src, length).fillna(0)
        # Calcular upper y lower
        change_src = [0 if change <= 0 else stddev[i] for i, change in enumerate(self.change(src))]
        upper = self.ema(change_src, len_param)
        change_src = [0 if change > 0 else stddev[i] for i, change in enumerate(self.change(src))]
        lower = self.ema(change_src, len_param)

        # Calcular RVI
        rvi = upper / (upper + lower) * 100
        rvi_ma = self.sma(rvi, len_param)

        return rvi.values, rvi_ma

    def get_channel(self, src, length):
        mid = np.sum(src[-length:]) / length
        slope = np.polyfit(range(length), src[-length:], 1)[0]
        intercept = mid - slope * ((length - 1) / 2)
        endy = intercept + slope * (length - 1)
        dev = np.sqrt(np.sum((src[-length:] - (slope * np.arange(length) + intercept)) ** 2) / length)
        return intercept, endy, dev, slope

    def calculate_channels(self, close, length=100, dev_len=2):
        src = np.array(close)
        y1, y2, dev, slope = self.get_channel(src, length)

        reglines = []
        for x in range(3):
            reglines.append((y1 + dev * dev_len * (x - 1), y2 + dev * dev_len * (x - 1)))

        return reglines

    def volatility_strat(self, close, atr_volatility):

        # Cargar datos históricos de precios de criptomonedas
        close_arr = np.array(close)

        # Crear un DataFrame a partir de las listas
        data = pd.DataFrame({'Close': close_arr})
        # Parámetros
        period_atr = 14  # Período para el indicador ATR
        period_rsi = 14  # Período para el indicador RSI
        period_stochrsi = 14  # Período para el indicador StochRSI
        overbought = 80  # Nivel de sobrecompra
        oversold = 20  # Nivel de sobreventas

        # Calcular indicadores
        data["RSI"] = talib.RSI(data["Close"], period_rsi)
        stochrsi = ta.STOCHRSI(data["Close"], period_stochrsi)
        data["StochRSI"] = stochrsi[-1]

        # Crear señales de trading

        # Condiciones de entrada

        buy_condition = (
                (data["RSI"] < oversold) &
                (data["StochRSI"] < oversold) &
                atr_volatility  # Alta volatilidad
        )
        # if atr_volatility.values[-1]:
        #     print(f"high volatility {data["ATR"].values[-1]} for {self.contract.symbol}")

        # Condiciones de salida
        sell_condition = (
                (data["RSI"] > overbought) &
                (data["StochRSI"] > overbought) &
                atr_volatility
        )

        return buy_condition.values[-1], sell_condition.values[-1]

    def rsi_fast(self, close: np.ndarray):
        period = 2
        rsi = talib.RSI(close, period)
        return rsi[-1] < 10, rsi[-1] > 91

    def rsi_strat(self, df: pandas.DataFrame):
        period = 14
        df['RSI'] = talib.RSI(df['Close'], period)
        df['Avg_RSI'] = df['RSI'].rolling(window=28).mean()

        rsi_buy = np.where((df['RSI'] < df['Avg_RSI'] * 0.5), True, False)
        rsi_sell = np.where((df['RSI'] > df['Avg_RSI'] * 1.5), True, False)

        last_true_buy = len(rsi_buy) - np.flip(rsi_buy).argmax() - 1 if np.any(rsi_buy) else -1
        last_true_sell = len(rsi_sell) - np.flip(rsi_sell).argmax() - 1 if np.any(rsi_sell) else -1

        last_ut_signal = None

        if last_true_buy > last_true_sell:
            last_ut_signal = "buy"
        elif last_true_buy < last_true_sell:
            last_ut_signal = "sell"

        return rsi_buy[-1], rsi_sell[-1], last_ut_signal

    def bollinger_strat(self, data):

        # overbought = 75
        # oversold = 25
        period = 14

        # data["RSI"] = talib.RSI(data["Close"], period)

        atr = talib.ATR(data["High"], data["Low"], data["Close"], period)
        atr_last_14_periods_mean = np.mean(atr[-14:])

        atr_volatility = atr.values[-2] > (atr_last_14_periods_mean * 1.4)
        atr_volatility_low = atr.values[-1] > (atr_last_14_periods_mean * 1.2)

        if atr_volatility:
            print(f"High volatity atr {atr.values[-2]} for {self.contract.symbol}")

        # # Calcular las Bandas de Bollinger
        # data['upper_band'], data['middle_band'], data['lower_band'] = talib.BBANDS(data['Close'], timeperiod=20,
        #                                                                            nbdevup=2, nbdevdn=2)

        # Generar señales de compra y venta
        # buy_condition = ((data["RSI"] < oversold) &
        #                  atr_volatility &
        #                  (data['Close'] < data['lower_band']))
        #
        # sell_condition = ((data["RSI"] > overbought) &
        #                   atr_volatility &
        #                   (data['Close'] > data['upper_band']))

        return atr_volatility_low, atr_last_14_periods_mean

    def bollinger_rsi_strat(self, data: pandas.DataFrame):
        # Calcular las Bandas de Bollinger
        data['upper_band'], data['middle_band'], data['lower_band'] = talib.BBANDS(data['Close'], timeperiod=20,
                                                                                   nbdevup=2, nbdevdn=2)
        buy_condition = (data['Close'] < data['lower_band'])
        sell_condition = (data['Close'] > data['upper_band'])

        return buy_condition.values[-2], sell_condition.values[-2]

    def macd_cobra_strat(self, closes: np.ndarray, highs: np.ndarray, lows: np.ndarray, opens: list):
        try:
            macd, signal, histogram = talib.MACD(closes, fastperiod=12, slowperiod=26, signalperiod=9)
            # ema = talib.EMA(closes, timeperiod=12)
            ema_low = talib.EMA(lows, timeperiod=12)
            ema_high = talib.EMA(highs, timeperiod=12)
            candle_type = self.get_candle_type(opens[-2], highs[-2], lows[-2], closes[-2])

            # buy = (ema_low[-2] < opens[-2] < ema_high[-2] < closes[-2] and histogram[-2] > 0 and 0 > macd[-2] > signal[-2] < 0
            #        and candle_type == BULLISH)
            # sell = (ema_high[-2] > opens[-2] > ema_low[-2] > closes[-2] and histogram[-2] < 0 and 0 < macd[-2] < signal[-2] > 0
            #         and candle_type == BEARISH)

            buy = np.where((ema_high > opens) & (opens > ema_low) & (ema_high < closes) &
                           (histogram > 0) & (0 > macd) & (macd > signal) & (signal < 0) &
                           (candle_type == BULLISH), True, False)

            sell = np.where((ema_high > opens) & (opens > ema_low) & (ema_low < closes) &
                            (histogram < 0) & (0 < macd) & (macd < signal) & (signal > 0) &
                            (candle_type == BEARISH), True, False)

            last_true_buy = len(buy) - np.flip(buy).argmax() - 1 if np.any(buy) else -1
            last_true_sell = len(sell) - np.flip(sell).argmax() - 1 if np.any(sell) else -1
            last_macd_cobra = ""
            if last_true_buy > last_true_sell:
                last_macd_cobra = "buy"
            elif last_true_buy < last_true_sell:
                last_macd_cobra = "sell"

            return buy[-2], sell[-2], last_macd_cobra
        except Exception as e:
            print(f"error macd_cobra_strat {e}")

    def get_atr(self, data):
        atr = talib.ATR(data["High"], data["Low"], data["Close"], 14)
        atr_last_14_periods_mean = np.mean(atr[-50:])
        atr_volatility = atr.values[-1] > (atr_last_14_periods_mean * 1.4)

        return atr_volatility, atr.values[-1]

    def adx_strat(self, close, low, high):
        adx = talib.ADX(high, low, close, timeperiod=14)  # timeperiod es el período para calcular el ADX
        adx_tendency = ""
        if adx[-3] < adx[-2]:
            adx_tendency = BULLISH
        elif adx[-3] > adx[-2]:
            adx_tendency = BEARISH
        # signal_buy = close[-3] < close[-2] and adx_tendency == BEARISH
        # signal_sell = close[-3] > close[-2] and adx_tendency == BULLISH

        return adx[-1], adx_tendency

    def volume_strat(self, data: pd.DataFrame, atr_high_factor, atr_low_factor):

        data['atr'] = talib.ATR(data["High"], data["Low"], data["Close"], 14)
        data['atr_mean'] = np.mean(data['atr'][-28:])

        # data['Volume_MA'] = data['Volume'].rolling(window=20).mean()
        # data['atr'] = data['Volume'].rolling(window=20).mean()
        # data['atr_mean'] = data['Volume'].rolling(window=20).mean()

        # Increasing signal: Volume ⬆️ and price ⬇️
        data['Buy_Signal'] = (data['atr'] > (data['atr_mean'] * atr_high_factor)) & (
                data['Close'] < data['Close'].shift(1))
        # Decreasing signal: Volume ⬇️ and price ⬆️
        data['Sell_Signal'] = (data['atr'] < (data['atr_mean'] * atr_low_factor)) & (
                data['Close'] > data['Close'].shift(1))
        buy = data['Buy_Signal'].values[-2]
        sell = data['Sell_Signal'].values[-2]

        last_true_buy = len(data['Buy_Signal']) - np.flip(data['Buy_Signal']).argmax() - 1 if np.any(
            data['Buy_Signal']) else -1
        last_true_sell = len(data['Sell_Signal']) - np.flip(data['Sell_Signal']).argmax() - 1 if np.any(
            data['Sell_Signal']) else -1
        last_volume_signal = None
        if last_true_buy > last_true_sell:
            last_volume_signal = "buy"
        elif last_true_buy < last_true_sell:
            last_volume_signal = "sell"

        return buy, sell, last_volume_signal

    def smoothrng(self, x, t, m):
        wper = t * 2 - 1
        avrng = talib.EMA(np.abs(x - x.shift(1)), timeperiod=t)
        smoothrng = talib.EMA(avrng, timeperiod=wper) * m

        return pd.Series(smoothrng)

    def rngfilt(self, x, r):
        rngfilt = x.copy()
        rngfilt = np.where(x > rngfilt.shift(1), np.where(x - r < rngfilt.shift(1), rngfilt.shift(1), x - r),
                           np.where(x + r > rngfilt.shift(1), rngfilt.shift(1), x + r))
        return pd.Series(rngfilt)

    def vishnu_patrick_strat(self, close: list):
        srcz = pd.Series(close)
        # Sampling Period
        per = 50  # Sampling period
        mult = 3.0  # Range Multiplier

        smrng = self.smoothrng(srcz, per, mult)
        filt = self.rngfilt(srcz, smrng)

        # Filter Direction
        upward = filt.copy()
        upward = upward.where(filt > filt.shift(1)).ffill(limit=None)
        upward = upward.where(filt < filt.shift(1), 0).ffill(limit=None)

        downward = filt.copy()
        downward = downward.where(filt < filt.shift(1)).ffill(limit=None)
        downward = downward.where(filt > filt.shift(1), 0).ffill(limit=None)

        # Break Outs
        long_cond = pd.Series([None] * len(srcz), index=srcz.index)
        short_cond = pd.Series([None] * len(srcz), index=srcz.index)

        for i in range(1, len(srcz)):
            if (srcz.iloc[i] > filt.iloc[i] and srcz.iloc[i] > srcz.iloc[i - 1] and upward.iloc[i] > 0 or
                    filt.iloc[i] < srcz.iloc[i] < srcz.iloc[i - 1] and upward.iloc[i] > 0):
                long_cond.iloc[i] = True
            else:
                long_cond.iloc[i] = False

            if (srcz.iloc[i] < filt.iloc[i] and srcz.iloc[i] < srcz.iloc[i - 1] and downward.iloc[i] > 0 or
                    filt.iloc[i] > srcz.iloc[i] > srcz.iloc[i - 1] and downward.iloc[i] > 0):
                short_cond.iloc[i] = True
            else:
                short_cond.iloc[i] = False

        cond_ini = long_cond.copy()
        cond_ini.iloc[0] = 0

        for i in range(1, len(srcz)):
            if long_cond.iloc[i]:
                cond_ini.iloc[i] = 1
            elif short_cond.iloc[i]:
                cond_ini.iloc[i] = -1
            else:
                cond_ini.iloc[i] = cond_ini.iloc[i - 1]

        long_condition = long_cond & (cond_ini.shift(1) == -1)
        short_condition = short_cond & (cond_ini.shift(1) == 1)

        if filt.iloc[-3] == filt.iloc[-2] == filt.iloc[-1]:
            print(f"Vishnu Filt SIDEWAY {self.contract.symbol}")

        return long_condition.values[-1] and filt.iloc[-1] < close[-1], short_condition.values[-1] and filt.iloc[-1] > \
               close[-1]

    def squeeze_momentum(self, data):
        length_kc = 20
        high_rolling = data['High'].rolling(length_kc).max()
        low_rolling = data['Low'].rolling(length_kc).min()
        typical_price = (high_rolling + low_rolling + data['Close']) / 3
        source = data['Close'] - typical_price.rolling(length_kc).mean()

        val = source.rolling(length_kc).apply(lambda x: linregress(np.arange(len(x)), x.values)[0])

        return val.values[-1]

    def today_is_sunday(self):
        current_date = datetime.today()
        sunday = current_date.weekday() == 6
        return sunday

    def on_new_york_session(self):
        current_hour = datetime.now(timezone.utc)
        utc5 = pytz.timezone('America/New_York')
        current_hour_utc5 = current_hour.replace(tzinfo=pytz.utc).astimezone(utc5)
        hour = current_hour_utc5.hour
        new_york = 7 <= hour <= 21
        if new_york:
            return True
        else:
            return False

    def send_email(self, subject, message):
        with smtplib.SMTP("smtp.gmail.com") as connection:
            connection.starttls()
            connection.login(user=MY_EMAIL, password=PASSWORD)
            connection.sendmail(
                from_addr=MY_EMAIL,
                to_addrs="candrelosolaz@gmail.com",
                msg=f"Subject: {subject}!\n\n{message}"
            )

    def _get_trend_type(self, ema):
        last_ema = ema[-10:]
        if all(last_ema.iloc[i] > last_ema.iloc[i + 1] for i in range(len(last_ema) - 1)):
            return BEARISH
        elif all(last_ema.iloc[i] < last_ema.iloc[i + 1] for i in range(len(last_ema) - 1)):
            return BULLISH
        else:
            return None

    def _get_ema_by_size(self, ema_size):

        """
        Compute the MACD and its Signal line.
        :return: The MACD and the MACD Signal value of the previous candlestick
        """

        close_list = []
        for candle in self.candles:
            close_list.append(candle.close)  # Use only the close price of each candlestick for the calculations
        closes = pd.Series(close_list)  # Converts the close prices list to a pandas Series.
        ema_result = closes.ewm(span=ema_size).mean()
        return ema_result

    def _get_ema_high_by_size(self, ema_size):

        """
        Compute the MACD and its Signal line.
        :return: The MACD and the MACD Signal value of the previous candlestick
        """

        close_list = []
        for candle in self.candles:
            close_list.append(candle.high)  # Use only the close price of each candlestick for the calculations
        closes = pd.Series(close_list)  # Converts the close prices list to a pandas Series.
        ema_result = closes.ewm(span=ema_size).mean()
        return ema_result

    def _get_ema_low_by_size(self, ema_size):

        """
        Compute the MACD and its Signal line.
        :return: The MACD and the MACD Signal value of the previous candlestick
        """

        close_list = []
        for candle in self.candles:
            close_list.append(candle.low)  # Use only the close price of each candlestick for the calculations
        closes = pd.Series(close_list)  # Converts the close prices list to a pandas Series.
        ema_result = closes.ewm(span=ema_size).mean()
        return ema_result

    def _rsi_ma(self, close_prices: list):
        rsi = talib.RSI(np.array(close_prices), timeperiod=14)
        rsi_ma = talib.EMA(rsi, timeperiod=14)

        return rsi, rsi_ma[-1]

    def _stochastich(self, high_prices: np.ndarray, low_prices: np.ndarray, close_prices: np.ndarray):
        fast_k, slow_d = talib.STOCH(high_prices, low_prices, close_prices,
                                     fastk_period=14, slowk_period=3, slowk_matype=0,
                                     slowd_period=3, slowd_matype=0)

        return fast_k[-1], slow_d[-1]

    def calc_smma(self, src_, length):
        smma = pd.Series(index=src_.index)
        smma[0] = src_.iloc[0]
        for i in range(1, len(src_)):
            smma.iloc[i] = (smma.iloc[i - 1] * (length - 1) + src_.iloc[i]) / length
        return smma

    def calc_zlema(self, src_, length):
        ema1 = src_.ewm(span=length, adjust=False).mean()
        ema2 = ema1.ewm(span=length, adjust=False).mean()
        d = ema1 - ema2
        return ema1 + d

    def session_zones(self, df: pd.DataFrame):
        window = len(df) - 10
        last_3h_session = df.iloc[window - 120:window]
        max_limit = last_3h_session['Close'].max()
        min_limit = last_3h_session['Close'].min()
        zones_size = (max_limit - min_limit) / 4
        resistance_zone = max_limit - zones_size
        support_zone = min_limit + zones_size
        # zone = 'BrokeResistance' if (max + zones_size) > df['Close'].values[-1] >= max else 'Resistance' if max > df['Close'].values[-1] >= resistance_zone else 'BrokeSupport' if (min - resistance_zone) < df['Close'].values[-1] <= min else 'Support' if min < df['Close'].values[-1] <= support_zone else 'Neutral'
        zone = ''
        if df['Close'].values[-1] >= max_limit:
            zone = 'BrokeResistance'
        elif max_limit > df['Close'].values[-1] >= resistance_zone:
            zone = 'Resistance'
        elif df['Close'].values[-1] <= min_limit:
            zone = 'BrokeSupport'
        elif min_limit < df['Close'].values[-1] <= support_zone:
            zone = 'Support'
        else:
            zone = "Neutral"

        return zone, resistance_zone, support_zone, max_limit, min_limit

    def _macd(self, close_prices: np.ndarray, fast: int, slow: int):

        macd, signal, _ = talib.MACD(close_prices, fastperiod=fast, slowperiod=slow, signalperiod=9)

        return macd[-1], signal[-1]

    def _stochastic_signal_cross(self, fast_k, slow_d):

        buy_stochastic = fast_k[-4] < slow_d[-4] and fast_k[-2] > slow_d[-2] and 0 < fast_k[-1] > slow_d[-1] > 0
        sell_stochastic = fast_k[-4] > slow_d[4] and fast_k[-2] < slow_d[-2] and 0 > fast_k[-1] < slow_d[-1] < 0

        return buy_stochastic, sell_stochastic

    def triple_ema_fibonashi(self, close_prices: np.ndarray, fast: int, mid: int, slow: int):
        ema_fast = talib.EMA(close_prices, timeperiod=fast)
        ema_mid = talib.EMA(close_prices, timeperiod=mid)
        ema_slow = talib.EMA(close_prices, timeperiod=slow)

        buy = (ema_fast[-1] > ema_mid[-1] > ema_slow[-1] and close_prices[-3] < ema_fast[-3] and
               close_prices[-2] > ema_fast[-2])
        sell = (ema_fast[-1] < ema_mid[-1] < ema_slow[-1] and close_prices[-3] > ema_fast[-3] and
                close_prices[-2] < ema_fast[-2])

        return buy, sell

    def rsi_kiss(self, close_prices: np.ndarray):
        period = 14
        rsi = talib.RSI(close_prices, period)
        ema_10 = talib.EMA(close_prices, timeperiod=5)
        ema_34 = talib.EMA(close_prices, timeperiod=8)

        buy = ema_10[-1] > ema_34[-1] and rsi[-6] > 55 < rsi[-5] and rsi[-4] > 55 > rsi[-3] and rsi[-2] > 55
        sell = ema_10[-1] < ema_34[-1] and rsi[-6] < 45 > rsi[-5] and rsi[-4] < 45 < rsi[-3] and rsi[-2] < 45

        return buy, sell

    def _vwap(self, df: pd.DataFrame, cumulative_period: int):
        # Definir el período
        df['typicalPrice'] = (df['High'] + df['Low'] + df['Close']) / 3
        df['typicalPriceVolume'] = df['typicalPrice'] * df['Volume']
        df['cumulativeTypicalPriceVolume'] = df['typicalPriceVolume'].rolling(window=cumulative_period).sum()
        df['cumulativeVolume'] = df['Volume'].rolling(window=cumulative_period).sum()
        df['vwapValue'] = df['cumulativeTypicalPriceVolume'] / df['cumulativeVolume']
        vwap_asc = df['vwapValue'].values[-3] < df['vwapValue'].values[-2] < df['vwapValue'].values[-1]
        vwap_desc = df['vwapValue'].values[-3] > df['vwapValue'].values[-2] > df['vwapValue'].values[-1]

        return vwap_asc, vwap_desc, df['vwapValue'].values[-1]

    def _vwap_reg(self, df: pd.DataFrame, cumulative_period: int, reg_period: int):
        # Definir el período
        df['typicalPrice'] = (df['High'] + df['Low'] + df['Close']) / 3
        df['typicalPriceVolume'] = df['typicalPrice'] * df['Volume']
        df['cumulativeTypicalPriceVolume'] = df['typicalPriceVolume'].rolling(window=cumulative_period).sum()
        df['cumulativeVolume'] = df['Volume'].rolling(window=cumulative_period).sum()
        df['vwapValue'] = df['cumulativeTypicalPriceVolume'] / df['cumulativeVolume']

        reg_vwap = self.linear_reg_series(df['vwapValue'], reg_period, 0)

        vwap_asc = reg_vwap[-3] < reg_vwap[-2] < reg_vwap[-1]
        vwap_desc = reg_vwap[-3] > reg_vwap[-2] > reg_vwap[-1]

        return vwap_asc, vwap_desc, df['vwapValue'].values[-1]

    def green_vs_red(self, open_prices, close_prices):
        lenght = 15

        open_last_20 = open_prices[-lenght:]
        close_last_20 = close_prices[-lenght:]
        green = 0
        red = 0
        for i in range(lenght):
            if open_last_20[i] < close_last_20[i]:
                green = green + 1
            else:
                red = red + 1

        return green > red, red > green, green, red


class TechnicalStrategy(Strategy):
    def __init__(self, client, contract: Contract, exchange: str, timeframe: str, balance_pct: float,
                 take_profit: float,
                 stop_loss: float, other_params: Dict):
        super().__init__(client, contract, exchange, timeframe, balance_pct, take_profit, stop_loss, "Technical")

        self._long_enable = other_params['long_enable']
        self._short_enable = other_params['short_enable']
        self._ut_key_value = other_params['ut_key_value']
        self._ut_atr_period = other_params['ut_atr_period']

    def _check_signal(self):

        """
        Compute technical indicators and compare their value to some predefined levels to know whether to go Long,
        Short, or do nothing.
        :return: 1 for a Long signal, -1 for a Short signal, 0 for no signal
        """
        try:
            candles = self.candles
            closes = []
            opens = []
            lows = []
            highs = []
            for candle in candles:
                closes.append(candle.close)
                opens.append(candle.open)
                lows.append(candle.low)
                highs.append(candle.high)

            close_arr = np.array(closes)
            low_arr = np.array(lows)
            high_arr = np.array(highs)
            data = pd.DataFrame({'Close': close_arr, 'Low': low_arr, 'High': high_arr})

            _, slope = self.get_slope(close_arr, 300)

            buy_heikin, sell_heikin, last_candle_type = self.heikin_ashi_strat(opens, highs, lows, closes, slope)

            atr_volatity, last_atr = self.get_atr(data)

            last_candle_close = closes[-1]

            long1 = buy_heikin
            short1 = sell_heikin

            take_profit_factor = 3
            stop_loss_factor = 1.8

            self.take_profit_long = last_candle_close + (take_profit_factor * last_atr)
            self.stop_loss_long = last_candle_close - (stop_loss_factor * last_atr)

            self.take_profit_short = last_candle_close - (take_profit_factor * last_atr)
            self.stop_loss_short = last_candle_close + (stop_loss_factor * last_atr)

            log_message = (f"TechnicalStrategy for {self.contract.symbol}\n "
                           f"last_candle_type {last_candle_type} \n "
                           f"Last close {last_candle_close} \n ")

            buy_message = f"{log_message} self.stop_loss_long {self.stop_loss_long} \n take_profit_long {self.take_profit_long}"
            sell_message = f"{log_message} self.stop_loss_short {self.stop_loss_short} \n take_profit_short {self.take_profit_short}"

            # return_long = 1 if self._long_enable == 1 else 0
            # return_short = -1 if self._short_enable == 1 else 0

            if long1:
                logger.info("Long1 " + buy_message)
                return 0
            elif short1:
                logger.info("Short1 " + sell_message)
                return 0

        except Exception as e:
            print(f"error signals {e}")
            return 0

    def check_trade(self, tick_type: str):

        """
        To be triggered from the websocket _on_message() methods. Triggered only yo once per candlestick to avoid
        constantly calculating the indicators. A trade can occur only if the is no open position at the moment.
        :param tick_type: same_candle or new_candle
        :return:
        """
        if tick_type == "new_candle" and not self.ongoing_position:
            signal_result = self._check_signal()
            # signal_result = 0

            if signal_result in [1, -1]:
                self._open_position(signal_result)

    # def get_last_volume_color(self):
    #     exchange = ccxt.binance()
    #     volume_historic = exchange.fetch_ohlcv(self.contract.symbol, self.tf)
    #     v_open = volume_historic[-1][1]
    #     v_close = volume_historic[-1][4]
    #
    #     if v_close > v_open:
    #         return "green"
    #     else:
    #         return "red"


class BreakoutStrategy(Strategy):
    def __init__(self, client, contract: Contract, exchange: str, timeframe: str, balance_pct: float,
                 take_profit: float,
                 stop_loss: float, other_params: Dict):
        super().__init__(client, contract, exchange, timeframe, balance_pct, take_profit, stop_loss, "Breakout")

        # self._rsi_ma_overbought = other_params['rsi_ma_overbought']
        # self._rsi_ma_oversold = other_params['rsi_ma_oversold']
        self._long_enable = other_params['long_enable']
        self._short_enable = other_params['short_enable']
        # self._ut_key_value = other_params['ut_key_value']
        # self._ut_atr_period = other_params['ut_atr_period']
        # self._test_mode = other_params['test_mode']
        # self._ut_key_value_parent = 3
        # self._ut_atr_period_parent = self._ut_atr_period
        # self._heikin_ashi = other_params['heikin_ashi']

    def _check_signal(self):

        """
        Compute technical indicators and compare their value to some predefined levels to know whether to go Long,
        Short, or do nothing.
        :return: 1 for a Long signal, -1 for a Short signal, 0 for no signal
        """
        try:
            candles = self.candles
            closes = []
            opens = []
            lows = []
            highs = []
            volume = []
            timestamps = []
            for candle in candles:
                closes.append(candle.close)
                opens.append(candle.open)
                lows.append(candle.low)
                highs.append(candle.high)
                volume.append(candle.volume)
                timestamps.append(candle.timestamp)

            timestamps_datetime = [datetime.fromtimestamp(timestamp / 1000) for timestamp in timestamps]
            formatted_timestamps = [timestamp.strftime("%Y-%m-%d %H:%M:%S") for timestamp in timestamps_datetime]

            close_arr = np.array(closes)
            open_arr = np.array(closes)
            low_arr = np.array(lows)
            high_arr = np.array(highs)
            volume_arr = np.array(volume)
            timestamp_arr = np.array(formatted_timestamps)
            data = pd.DataFrame({
                'Open': open_arr,
                'Close': close_arr,
                'Low': low_arr,
                'High': high_arr,
                'Volume': volume_arr,
                'date': timestamp_arr
            })
            zone, resistance_limit, support_limit, max_limit, min_limit = self.session_zones(data)
            cumulative_period = 0
            match self.tf:
                case "1m": cumulative_period = 110
                case "5m": cumulative_period = 80
                case "15m": cumulative_period = 60
                case "30m": cumulative_period = 60
                case "1h": cumulative_period = 30
                case "4h": cumulative_period = 30

            vwap_asc, vwap_desc, vwap_value = self._vwap(data, cumulative_period)
            # reg_period = int(round(cumulative_period / 2, 0))
            # vwap_asc, vwap_desc, _ = self._vwap_reg(data, cumulative_period, reg_period)
            vwap_asc_20, vwap_desc_20, vwap_value_20 = self._vwap_reg(data, 30, 10)

            green_win, red_win, green_count, red_count = self.green_vs_red(opens, closes)

            # Volume LSMA
            volume_length = 50 if self.tf == "1m" else 60
            lsma_relative_volume = self.linear_reg_series(data['Volume'], volume_length, 0)
            volume_above_sma = data['Volume'].iloc[-3] > lsma_relative_volume[-3] > 0

            vol_sma_asc = lsma_relative_volume[-5] < lsma_relative_volume[-4] < lsma_relative_volume[-3]
            vol_sma_desc = lsma_relative_volume[-3] < lsma_relative_volume[-4] < lsma_relative_volume[-5]

            volume_asc = data['Volume'].iloc[-5] < data['Volume'].iloc[-4] < data['Volume'].iloc[-3]
            volume_asc_last = data['Volume'].iloc[-3] < data['Volume'].iloc[-2]
            volume_desc = data['Volume'].iloc[-3] < data['Volume'].iloc[-4] < data['Volume'].iloc[-5]
            volume_desc_last = data['Volume'].iloc[-2] < data['Volume'].iloc[-3]

            candle_type = self.get_candle_type(opens[-2], highs[-2], lows[-2], closes[-2])
            previous_candle_type = self.get_candle_type(opens[-4], highs[-4], lows[-4], closes[-4])
            last_candle_close = closes[-1]
            # rsi = talib.RSI(close_arr, timeperiod=14)
            # ema_21 = talib.EMA(close_arr, timeperiod=21)
            # ema_50 = talib.EMA(close_arr, timeperiod=50)
            # ema_200 = talib.EMA(close_arr, timeperiod=200)
            _, slope_180_d, trend_180 = self.get_slope(close_arr, 180)
            slope_120, slope_120_d, trend_120 = self.get_slope(close_arr, 120)
            slope_28, slope_28_d, trend_28 = self.get_slope(close_arr, 28)
            slope_10, slope_10_d, trend_10 = self.get_slope(close_arr, 10)
            atr_volatility, atr_mean = self.bollinger_strat(data)
            adx, adx_tendency = self.adx_strat(close_arr, low_arr, high_arr)

            buy_volume, sell_volume, last_volume_signal = self.volume_strat(data, 1.3, 0.7)
            # buy_triple_ema, sell_triple_ema = self.triple_ema_fibonashi(close_arr, 21, 50, 200)
            crosses_vwap_up = opens[-3] < vwap_value < closes[-3]
            crosses_vwap_down = opens[-3] > vwap_value > closes[-3]

            crosses_vwap_up_20 = opens[-3] < vwap_value_20 < closes[-3]
            crosses_vwap_down_20 = opens[-3] > vwap_value_20 > closes[-3]
            # rsi_buy, rsi_sell, last_rsi = self.rsi_strat(data)

            green = opens[-2] < closes[-2]
            red = opens[-2] > closes[-2]

            bull_condition = ("Resistance" !=  zone != "BrokeResistance" and volume_asc and closes[-2] > vwap_value < opens[-2] and
                              closes[-4] < vwap_value and vwap_asc_20 and volume_above_sma and green_win)
            sell_condition = ("Support" != zone != "BrokeSupport" and volume_desc and closes[-2] < vwap_value > opens[-2] and
                              closes[-4] > vwap_value and vwap_desc_20 and red_win)

            # bien +++
            long6 = crosses_vwap_up and bull_condition and vol_sma_asc and slope_10_d > 0
            # mal -
            short6 = crosses_vwap_down and sell_condition and vol_sma_desc and slope_10_d < 0

            long7 = (zone != "BrokeResistance" and vwap_value_20 < vwap_value < last_candle_close and long6)
            short7 = (zone != "BrokeSupport" and vwap_value_20 > vwap_value > last_candle_close and short6)

            # bien +
            long9 = (zone != "BrokeResistance" and vwap_value_20 < vwap_value < last_candle_close and vwap_asc_20 and
                     crosses_vwap_up and volume_above_sma and green and candle_type == BULLISH and volume_asc and
                     vol_sma_asc)
            short9 = (zone != "BrokeSupport" and vwap_value_20 > vwap_value > last_candle_close and vwap_desc_20 and
                      crosses_vwap_down and red and candle_type == BEARISH and volume_desc and volume_desc and
                      vol_sma_desc)
            
            long10 = ("Resistance" != zone != "BrokeResistance" and volume_asc_last and red_win and
                      trend_10 == STRONG_BULLISH == trend_28 and vwap_value < last_candle_close and
                      slope_28_d > 0 < slope_180_d and vol_sma_asc)
            # ?-
            short10 = ("Support" != zone != "BrokeSupport" and volume_desc_last and green_win and
                       trend_10 == STRONG_BEARISH == trend_28 and vwap_value > last_candle_close and
                       slope_180_d < 0 > slope_28_d and vol_sma_desc)

            #funciona mal
            long11 = (slope_180_d > 0 < slope_28_d and "Resistance" != zone != "BrokeResistance" and bull_condition)
            short11 = (slope_180_d < 0 > slope_28_d and "Support" != zone != "BrokeSupport" and sell_condition)

            # funciona bien
            long12 = ("Resistance" != zone != "BrokeResistance" and slope_10_d > 0 and volume_asc and green_win and
                      vwap_asc and green and last_volume_signal == "buy" and candle_type == BULLISH and vwap_asc_20
                      and slope_120_d > 0 and vwap_value_20 > vwap_value)
            short12 = ("Support" != zone != "BrokeSupport" and slope_10_d < 0 and volume_asc and red_win and
                       vwap_desc and red and last_volume_signal == "sell" and candle_type == BEARISH and vwap_desc_20
                       and slope_120_d < 0 and vwap_value_20 < vwap_value)

            long13 = ("Resistance" != zone != "BrokeResistance" and slope_10_d > 0 and volume_asc and green_win
                      and vwap_asc and green and volume_above_sma and candle_type == BULLISH and adx_tendency == BEARISH
                      and not vwap_desc_20 and last_volume_signal == "buy" and vwap_value_20 > vwap_value)
            #2 bien 1 mal
            short13 = ("Support" != zone != "BrokeSupport" and slope_10_d < 0 and volume_desc and red_win and
                       vwap_desc and red and not volume_above_sma and candle_type == BEARISH and adx_tendency == BULLISH
                       and not vwap_asc_20 and last_volume_signal == "sell" and vwap_value_20 < vwap_value)
            #bien
            long14 = ("Resistance" != zone != "BrokeResistance" and slope_10_d > 0 < slope_28_d and volume_desc and
                      green_win and vwap_asc and green and last_volume_signal == "buy" and adx_tendency == BULLISH and
                      vwap_value_20 > vwap_value)
            #bien
            short14 = ("Support" != zone != "BrokeSupport" and slope_10_d < 0 > slope_28_d and vol_sma_asc and red_win and
                       vwap_desc and red and last_volume_signal == "sell" and adx_tendency == BEARISH and
                       vwap_value_20 < vwap_value)

            # bien
            long15 = ("Resistance" != zone != "BrokeResistance" and (crosses_vwap_up or crosses_vwap_up_20) and
                      slope_120_d > 0 and volume_asc and last_candle_close > vwap_value)
            short15 = ("Support" != zone != "BrokeSupport" and (crosses_vwap_down or crosses_vwap_down_20) and
                       slope_120_d < 0 and volume_desc and last_candle_close < vwap_value)

            long16 = ("Resistance" != zone != "BrokeResistance" and vwap_asc and green_win and vol_sma_asc and
                      crosses_vwap_up and vwap_asc_20 and slope_120_d > 0)
            short16 = ("Support" != zone != "BrokeSupport" and vwap_desc and red_win and vol_sma_desc and
                       crosses_vwap_down and vwap_desc_20 and slope_120_d < 0)

            # max_profit = max( max_limit, (last_candle_close + (last_candle_close - ema_200[-1])))
            self.take_profit_long = max_limit
            self.stop_loss_long = last_candle_close - ((self.take_profit_long - last_candle_close) * 0.85)

            # min_profit = min(support_limit, (last_candle_close - (ema_200[-1] - last_candle_close)))
            self.take_profit_short = min_limit
            self.stop_loss_short = last_candle_close + ((last_candle_close - self.take_profit_short) * 0.85)

            # if zone == "Resistance" and ema_21[-1] < last_candle_close < max_limit:
            #     pass
            # long5 = short5 = False
            # if zone == "Neutral":
            #     long5 = (buy_triple_ema and last_candle_close > ema_200[-1] < vwap_value and slope_120 > 2 and
            #              trend_28 == STRONG_BULLISH and vwap_asc and last_volume_signal == "buy" and
            #              closes[-2] <= last_candle_close > ema_21[-1])
            #     short5 = (sell_triple_ema and last_candle_close < ema_200[-1] > vwap_value and slope_120 < -2 and
            #               trend_28 == STRONG_BEARISH and vwap_desc and last_volume_signal == "sell" and
            #               closes[-2] >= last_candle_close < ema_21[-1])
            #     self.take_profit_long = last_candle_close + (last_candle_close - ema_200[-1])
            #     self.stop_loss_long = ema_200[-1]
            #
            #     self.take_profit_short = last_candle_close - (ema_200[-1] - last_candle_close)
            #     self.stop_loss_short = last_candle_close + ((ema_200[-1] - last_candle_close) * 0.7)
            #
            # if zone == "Support" and min_limit < last_candle_close < ema_50[-1]:
            #     pass


            # @todo anadir validaciones chat gpt, gap, volumen relative, etc
            # @todo crear un probador de estrategias con los datos historicos de peridos especificos

            log_message = (f"Breakout for {self.contract.symbol} {self.tf}\n "
                           f"atr_volatility {atr_volatility} \n "
                           f"adx_tendency {adx_tendency} \n "
                           f"atr_mean {atr_mean} \n "
                           f"candle_type {candle_type} \n "
                           f"candle green {green} \n "
                           f"candle green wins {green_win} \n "
                           f"candle green_count {green_count} \n "
                           f"candle red wins {red_win} \n "
                           f"candle red_count {red_count} \n "
                           f"candle red {red} \n "
                           f"vwap period {cumulative_period} \n "
                           f"vwap period {cumulative_period} \n "
                           f"Entry price {last_candle_close} \n "
                           # f"last_rsi {last_rsi} \n "
                           f"last_volume_signal {last_volume_signal} \n "
                           f"max_limit {max_limit} \n "
                           f"previous_candle_type {previous_candle_type} \n "
                           f"resistance_limit {resistance_limit} \n "
                           f"support_limit {support_limit} \n "
                           f"min_limit {min_limit} \n "
                           f"slope_10 {slope_10_d} \n "
                           f"trend_10 {trend_10} \n "
                           f"slope_28 {slope_28_d} \n "
                           f"trend_28 {trend_28} \n "
                           f"slope_120 {slope_120_d} \n "
                           f"trend_120 {trend_120} \n "
                           f"slope_180 {slope_180_d} \n "
                           f"trend_180 {trend_180} \n "
                           f"timeframe {self.tf} \n "
                           f"vwap_value {vwap_value} \n "
                           f"vwap_asc_20 {vwap_asc_20} \n "
                           f"vwap_desc_20 {vwap_desc_20} \n "
                           f"vwap_asc {vwap_asc} \n "
                           f"vwap_desc {vwap_desc} \n "
                           f"volume_asc {volume_asc} \n "
                           f"volume_desc {volume_desc} \n "
                           f"volume_above_sma {volume_above_sma} \n "
                           f"vol_sma_asc {vol_sma_asc} \n "
                           f"vol_sma_desc {vol_sma_desc} \n "
                           f"zone {zone} \n")
            sl_tp_long_message = f"self.stop_loss_long {self.stop_loss_long}\n take_profit_long {self.take_profit_long}"
            sl_tp_short_message = f"self.stop_loss_short {self.stop_loss_short}\n take_profit_short {self.take_profit_short}"
            buy_message = f"{log_message} {sl_tp_long_message}"
            sell_message = f"{log_message} {sl_tp_short_message}"

            return_long = 1 if self._long_enable == 1 else 0
            return_short = -1 if self._short_enable == 1 else 0
            
            if long6:
                logger.info("Long6 " + buy_message)
                toaster = Notification(app_id="New order on Binance Long6",
                                       title=f"{self.contract.symbol} Long6",
                                       msg=f"{sl_tp_long_message}",
                                       duration=f"long",
                                       icon=ICON_LONG)
                toaster.set_audio(audio.LoopingAlarm, loop=False)
                toaster.show()
                # return return_long
            elif short6:
                logger.info("Short6 " + sell_message)
                toaster = Notification(app_id="New order on Binance Short6",
                                       title=f"{self.contract.symbol} Short6",
                                       msg=f"{sl_tp_short_message}",
                                       duration=f"long",
                                       icon=ICON_SHORT)
                toaster.set_audio(audio.LoopingAlarm, loop=False)
                toaster.show()
                # return return_short
                
            elif long7:
                logger.info("Long7 " + buy_message)
                toaster = Notification(app_id="New order on Binance Long7",
                                       title=f"{self.contract.symbol} Long7",
                                       msg=f"{sl_tp_long_message}",
                                       duration=f"long",
                                       icon=ICON_LONG)
                toaster.set_audio(audio.LoopingAlarm, loop=False)
                toaster.show()
                # return return_long
            elif short7:
                logger.info("Short7 " + sell_message)
                toaster = Notification(app_id="New order on Binance Short7",
                                       title=f"{self.contract.symbol} Short7",
                                       msg=f"{sl_tp_short_message}",
                                       duration=f"long",
                                       icon=ICON_SHORT)
                toaster.set_audio(audio.LoopingAlarm, loop=False)
                toaster.show()
                # return return_short

            elif long9:
                logger.info("long9 " + buy_message)
                toaster = Notification(app_id="New order on Binance Long9",
                                       title=f"{self.contract.symbol} Long9",
                                       msg=f"{sl_tp_long_message}",
                                       duration=f"long",
                                       icon=ICON_LONG)
                toaster.set_audio(audio.LoopingAlarm, loop=False)
                toaster.show()
                # return return_long
            elif short9:
                logger.info("Short9 " + sell_message)
                toaster = Notification(app_id="New order on Binance Short9",
                                       title=f"{self.contract.symbol} Short9",
                                       msg=f"{sl_tp_short_message}",
                                       duration=f"long",
                                       icon=ICON_SHORT)
                toaster.set_audio(audio.LoopingAlarm, loop=False)
                toaster.show()
                # return return_short
                
            elif long10:
                logger.info("long10 " + buy_message)
                toaster = Notification(app_id="New order on Binance Long10",
                                       title=f"{self.contract.symbol} Long10",
                                       msg=f"{sl_tp_long_message}",
                                       duration=f"long",
                                       icon=ICON_LONG)
                toaster.set_audio(audio.LoopingAlarm, loop=False)
                toaster.show()
                # return return_long
            elif short10:
                logger.info("Short10 " + sell_message)
                toaster = Notification(app_id="New order on Binance Short10",
                                       title=f"{self.contract.symbol} Short10",
                                       msg=f"{sl_tp_short_message}",
                                       duration=f"long",
                                       icon=ICON_SHORT)
                toaster.set_audio(audio.LoopingAlarm, loop=False)
                toaster.show()
                # return return_short

            elif long11:
                logger.info("Long11 " + buy_message)
                toaster = Notification(app_id="New order on Binance Long11",
                                       title=f"{self.contract.symbol} Long11",
                                       msg=f"{sl_tp_long_message}",
                                       duration=f"long",
                                       icon=ICON_LONG)
                toaster.set_audio(audio.LoopingAlarm, loop=False)
                toaster.show()
                # return return_long
            elif short11:
                logger.info("Short11 " + sell_message)
                toaster = Notification(app_id="New order on Binance Short11",
                                       title=f"{self.contract.symbol} Short11",
                                       msg=f"{sl_tp_short_message}",
                                       duration=f"long",
                                       icon=ICON_SHORT)
                toaster.set_audio(audio.LoopingAlarm, loop=False)
                toaster.show()
                # return return_short
                
            elif long12:
                logger.info("Long12 " + buy_message)
                toaster = Notification(app_id="New order on Binance Long12",
                                       title=f"{self.contract.symbol} Long12",
                                       msg=f"{sl_tp_long_message}",
                                       duration=f"long",
                                       icon=ICON_LONG)
                toaster.set_audio(audio.LoopingAlarm, loop=False)
                toaster.show()
                return return_long
            elif short12:
                logger.info("Short12 " + sell_message)
                toaster = Notification(app_id="New order on Binance Short12",
                                       title=f"{self.contract.symbol} Short12",
                                       msg=f"{sl_tp_short_message}",
                                       duration=f"long",
                                       icon=ICON_SHORT)
                toaster.set_audio(audio.LoopingAlarm, loop=False)
                toaster.show()
                return return_short

            elif long13:
                logger.info("long13 " + buy_message)
                toaster = Notification(app_id="New order on Binance Long13",
                                       title=f"{self.contract.symbol} Long13",
                                       msg=f"{sl_tp_long_message}",
                                       duration=f"long",
                                       icon=ICON_LONG)
                toaster.set_audio(audio.LoopingAlarm, loop=False)
                toaster.show()
                # return return_long
            elif short13:
                logger.info("Short13 " + sell_message)
                toaster = Notification(app_id="New order on Binance Short13",
                                       title=f"{self.contract.symbol} Short13",
                                       msg=f"{sl_tp_short_message}",
                                       duration=f"long",
                                       icon=ICON_SHORT)
                toaster.set_audio(audio.LoopingAlarm, loop=False)
                toaster.show()
                # return return_short
                
            elif long14:
                logger.info("long14 " + buy_message)
                toaster = Notification(app_id="New order on Binance Long14",
                                       title=f"{self.contract.symbol} Long14",
                                       msg=f"{sl_tp_long_message}",
                                       duration=f"long",
                                       icon=ICON_LONG)
                toaster.set_audio(audio.LoopingAlarm, loop=False)
                toaster.show()
                return return_long
            elif short14:
                logger.info("Short14 " + sell_message)
                toaster = Notification(app_id="New order on Binance Short14",
                                       title=f"{self.contract.symbol} Short14",
                                       msg=f"{sl_tp_short_message}",
                                       duration=f"long",
                                       icon=ICON_SHORT)
                toaster.set_audio(audio.LoopingAlarm, loop=False)
                toaster.show()
                return return_short
            
            elif long15:
                logger.info("long15 " + buy_message)
                toaster = Notification(app_id="New order on Binance Long15",
                                       title=f"{self.contract.symbol} Long15",
                                       msg=f"{sl_tp_long_message}",
                                       duration=f"long",
                                       icon=ICON_LONG)
                toaster.set_audio(audio.LoopingAlarm, loop=False)
                toaster.show()
                # return return_long
            elif short15:
                logger.info("Short15 " + sell_message)
                toaster = Notification(app_id="New order on Binance Short15",
                                       title=f"{self.contract.symbol} Short15",
                                       msg=f"{sl_tp_short_message}",
                                       duration=f"long",
                                       icon=ICON_SHORT)
                toaster.set_audio(audio.LoopingAlarm, loop=False)
                toaster.show()
                # return return_short
                
            elif long16:
                logger.info("long16 " + buy_message)
                toaster = Notification(app_id="New order on Binance Long16",
                                       title=f"{self.contract.symbol} Long16",
                                       msg=f"{sl_tp_long_message}",
                                       duration=f"long",
                                       icon=ICON_LONG)
                toaster.set_audio(audio.LoopingAlarm, loop=False)
                toaster.show()
                # return return_long
            elif short16:
                logger.info("Short16 " + sell_message)
                toaster = Notification(app_id="New order on Binance Short16",
                                       title=f"{self.contract.symbol} Short16",
                                       msg=f"{sl_tp_short_message}",
                                       duration=f"long",
                                       icon=ICON_SHORT)
                toaster.set_audio(audio.LoopingAlarm, loop=False)
                toaster.show()
                # return return_short

            else:
                return 0

        except Exception as e:
            print(f"error signals breakout {e}")
            return 0

    def check_trade(self, tick_type: str):

        """
        To be triggered from the websocket _on_message() methods
        :param tick_type: same_candle or new_candle
        :return:
        """
        try:
            # if tick_type == "new_candle":
            if tick_type == "new_candle" and self.verified:
                if not self.ongoing_position:
                    signal_result = self._check_signal()
                    # if True:
                    # if not self.on_new_york_session() or self.today_is_sunday():
                    #    signal_result = 0
                    if signal_result in [1, -1]:
                        self._open_position(signal_result)
                else:
                    position_data = self.client.get_open_positions()
                    for position in position_data:
                        if position['symbol'] == self.contract.symbol:
                            if "positionAmt" in position:
                                if float(position['positionAmt']) > 0:
                                    self.position_quantity = float(position['positionAmt'])
                                    break
            if not self.verified:
                self.calculate_gap_up_percentage()
        except Exception as e:
            print(f"error check_trade {e}")
            return 0
