```sh
pip install -r requirements.txt
```

Args:
            start (str)                 : Start date, eg. 2021-12-01
            end (str)                   : End date  , eg. 2023-08-19
            symbol (str)                : Dash seperated pair symbol. eg. BTC-USDT for spot and futures, BTCUSD_PERP for coin-margined futures
            tf (str, optional)          : Candle timeframe. Defaults to '1h'. Not used for metrics.
            market_type (str, optional) : Spot or futures. Defaults to 'futures'.
            margin_type (str, optional) : Futures only, um, cm (usdt margin, coin margin). Defaults to 'um'.
            data_type (str, optional)   : Spot     -> aggTrades, klines, trades
                                          Futures  -> aggTrades, indexPriceKlines, klines, markPriceKlines,  premiumIndexKlines, trades, metrics]. Defaults to 'klines'.
            worker_count (int, optional): Number of download jobs to run paralel. Defaults to 4.
            force_daily (bool, optional): Force daily data. Defaults to False. Some data is only available daily. eg. metrics, so we force daily for those.





Binance Futures BTCUST data starts at: 2020-01

Check tickers for coin-margined futures. eg. BTCUSD_PERP

https://data.binance.vision/data/futures/cm/daily/metrics/BTCUSD_PERP/BTCUSD_PERP-metrics-2023-08-19.zip
