from BulkBase import BulkBase

if __name__ == "__main__":
    binanceDownloader = BulkBase(
        start="2019-01-01",
        end="2023-08-21",
        market_type="futures",
        worker_count=10,
        data_type="klines",
        symbol="BTC-USDT",
        tf="1h",
        force_daily=False,
        merge=True,
    )

    binanceDownloader.run()
