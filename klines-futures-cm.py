from BulkBase import BulkBase, get_days, get_months

if __name__ == "__main__":
    binanceDownloader = BulkBase(
        start="2019-06-01",
        end="2023-08-18",
        market_type="futures",
        margin_type="cm",
        data_type="klines",
        symbol="BTCUSD_PERP",
        tf="1h",
        force_daily=False,
        merge=True,
    )

    binanceDownloader.run()
