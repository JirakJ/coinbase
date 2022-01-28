import time
from datetime import datetime, timedelta

import pandas as pd
import requests

timeframes = {
    "1m": [60, "1 min"], #supported by exchange
    "3m": [60, "3 min"],
    "5m": [300, "5 min"], #supported by exchange
    "15m": [900, "15 min"], #supported by exchange
    "30m": [900, "30 min"],
    "1h": [3600, "1h"], #supported by exchange
    "2h": [3600, "2h"],
    "4h": [3600, "4h"],
    "6h": [21600, "6h"],
    "8h": [3600, "8s"],
    "12h": [21600, "12h"], #supported by exchange
    "1d": [86400, "1D"], #supported by exchange
    "3d": [86400, "3D"],
    "1w": [86400, "7D"],
    "1M": [86400, "30D"],
}

class CoinBase():
    def __init__(self, apiUrl="https://api.exchange.coinbase.com"):
        self.apiUrl = apiUrl

    @property
    def symbols(self) -> []:
        response = requests.get(f"{self.apiUrl}/products", headers={"content-type": "application/json"})
        symbols = []
        for pair in response.json():
            symbols.append(pair["id"])
        return symbols

    def fetch_ohlcv(self, symbol, timeframe="2h", limit=300, from_date='2022-01-28 12:00:00.000000'):
        barSize = timeframes.get(timeframe)
        timeEnd = datetime.now()
        delta = timedelta(seconds=int(barSize[0]))
        if limit > 300:
            # print("Overridden limit: Sorry maximum limit is 300")
            limit = 300
        elif limit < 5:
            # print("Overridden limit: Sorry minimum limit is 1")
            limit = 5
        date_time_obj = datetime.strptime(from_date, '%Y-%m-%d %H:%M:%S.%f')
        frames = []
        while True:
            timeStart = timeEnd - (limit * delta)
            timeStart = timeStart.isoformat()
            timeEnd = timeEnd.isoformat()

            parameters = {
                "start": timeStart,
                "end": timeEnd,
                "granularity": barSize[0]
            }

            data = requests.get(f"{self.apiUrl}/products/{symbol}/candles", params=parameters,
                                headers={"content-type": "application/json"})
            time.sleep(1/3)
            df = pd.DataFrame(data.json(), columns=["time", "low", "high", "open", "close", "volume"])
            # print(df.head())

            if limit == 5:
                frames.append(df.tail(1))
            else:
                frames.append(df)
            if date_time_obj > datetime.fromisoformat(timeStart) or limit < 50:
                break
            else:
                timeEnd = datetime.fromisoformat(timeStart)

        df = pd.concat(frames)
        df.reset_index(inplace=True)
        df["date"] = pd.to_datetime(df["time"], unit="s")
        df.set_index("date", inplace=True)
        # print(df.head())
        df = df.resample(barSize[1]).agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "mean"
        })

        df.reset_index(inplace=True)

        df.dropna()
        df = df.loc[(df['date'] >= from_date)]
        df.reset_index(inplace=True)

        df = df.iloc[-limit:]
        df.reset_index(inplace=True)

        # print(df.head())
        rows = []
        for index, row in df[["date", "low", "high", "open", "close", "volume"]].iterrows():
            rows.append([
                row["date"],
                row["open"],
                row["high"],
                row["low"],
                row["close"],
                row["volume"],
            ])
        return rows

    def fetch_ohlc(self, symbol, timeframe="2h", limit=300, from_date='2021-12-01 02:00:00.000000'):
        barSize = timeframes.get(timeframe)
        timeEnd = datetime.now()
        delta = timedelta(seconds=int(barSize[0]))
        if limit > 300:
            limit = 300
        elif limit < 5:
            limit = 5
        date_time_obj = datetime.strptime(from_date, '%Y-%m-%d %H:%M:%S.%f')
        frames = []
        while True:
            timeStart = timeEnd - (limit * delta)
            timeStart = timeStart.isoformat()
            timeEnd = timeEnd.isoformat()

            parameters = {
                "start": timeStart,
                "end": timeEnd,
                "granularity": barSize[0]
            }

            data = requests.get(f"{self.apiUrl}/products/{symbol}/candles", params=parameters,
                                headers={"content-type": "application/json"})
            df = pd.DataFrame(data.json(), columns=["time", "low", "high", "open", "close", "volume"])
            if limit == 5:
                frames.append(df.tail(1))
            else:
                frames.append(df)
            if date_time_obj > datetime.fromisoformat(timeStart) or limit < 50:
                break
            else:
                timeEnd = datetime.fromisoformat(timeStart)

        df = pd.concat(frames)
        df.reset_index(inplace=True)
        df["date"] = pd.to_datetime(df["time"], unit="s")

        df.set_index("date", inplace=True)

        df = df.resample(barSize[1]).agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "mean"
        })

        df.reset_index(inplace=True)

        df.dropna()
        df = df.loc[(df['date'] >= from_date)]
        df.reset_index(inplace=True)

        rows = []
        for index, row in df[["date", "low", "high", "open", "close"]].iterrows():
            rows.append([
                row["date"],
                row["open"],
                row["high"],
                row["low"],
                row["close"],
            ])
        return rows

    def fetch_ticker(self, symbol) -> str:
        data = requests.get(f"{self.apiUrl}/products/{symbol}/ticker",
                            headers={"content-type": "application/json"})
        return data.json()



if __name__ == '__main__':
    coinbase = CoinBase()

    # print(coinbase.symbols)
    # print(coinbase.fetch_ticker("DOGE-USDT"))
    print(coinbase.fetch_ohlcv("DOGE-USDT", timeframe='1m', limit=5000))
    print(coinbase.fetch_ohlcv("DOGE-USDT", timeframe='3m', limit=5000))
    # print(coinbase.fetch_ohlc("DOGE-USDT", timeframe='3m', limit=5000))


