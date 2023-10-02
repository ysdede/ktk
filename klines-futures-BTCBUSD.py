from BulkBase import BulkBase

if __name__ == "__main__":
    binanceDownloader = BulkBase(
        start="2019-01-01",
        end="today",
        market_type="futures",
        margin_type="um",
        worker_count=10,
        data_type="klines",
        symbol="BTC-BUSD",
        tf="1h",
        force_daily=False,
        merge=True,
    )

    binanceDownloader.run()
