from BulkBase import BulkBase

if __name__ == "__main__":
    binanceDownloader = BulkBase(
        start="2022-06-01",
        end="today",
        market_type="futures",
        margin_type="cm",
        data_type="metrics",
        symbol="BTCUSD_PERP",   # cm: BTCUSD_PERP     BTCUSD_210924   BTCUSD_211231   BTCUSD_220325   BTCUSD_220624   BTCUSD_220930   BTCUSD_221230   BTCUSD_230331   BTCUSD_230630   BTCUSD_230929   BTCUSD_231229
        worker_count=16,
        force_daily=True,
        merge=True,
    )

    binanceDownloader.run()
