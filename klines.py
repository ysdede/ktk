from BulkBase import BulkBase, get_days, get_months

if __name__ == "__main__":
    binanceDownloader = BulkBase(
        start="2022-10-01",
        end="2023-08-18",
        market_type="futures",
        
        data_type="klines",
        symbol="BTC-USDT",
        tf="1h",
        force_daily=False,
        merge=True,
    )

    binanceDownloader.run()

