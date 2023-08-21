from BulkBase import BulkBase

if __name__ == "__main__":
    binanceDownloader = BulkBase(
        start="2021-11-01",
        end="2023-08-18",
        market_type="futures",
        margin_type="um",
        data_type="metrics",
        symbol="BTC-USDT",
        worker_count=16,
        force_daily=True,
        merge=True,
    )

    binanceDownloader.run()
