from BulkBase import BulkBase

if __name__ == "__main__":
    binanceDownloader = BulkBase(
        start="2022-11-01",
        end="today",
        market_type="futures",
        margin_type="um",
        data_type="metrics",
        symbol="BTCUSDT",  # um: BTCUSDT   BTCUSDT_211231  BTCUSDT_220325  BTCUSDT_220624  BTCUSDT_220930  BTCUSDT_221230  BTCUSDT_230331  BTCUSDT_230630  BTCUSDT_230929  BTCUSDT_231229  
        worker_count=16,
        force_daily=True,
        merge=True,
    )

    binanceDownloader.run()
