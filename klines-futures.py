from BulkBase import BulkBase

symbols = [
    "BTCUSD_210924",
    "BTCUSD_211231",
    "BTCUSD_220325",
    "BTCUSD_220624",
    "BTCUSD_220930",
    "BTCUSD_221230",
    "BTCUSD_230331",
    "BTCUSD_230630",
    "BTCUSD_230929",
    "BTCUSD_231229",
    "BTCUSDT_210924",
    "BTCUSDT_211231",
    "BTCUSDT_220325",
    "BTCUSDT_220624",
    "BTCUSDT_220930",
    "BTCUSDT_221230",
    "BTCUSDT_230331",
    "BTCUSDT_230630",
    "BTCUSDT_230929",
    "BTCUSDT_231229",
    "BTCUSD_PERP",
]

if __name__ == "__main__":
    binanceDownloader = BulkBase(
        start="2019-06-01",
        end="2023-08-18",
        market_type="futures",
        margin_type="um",
        data_type="klines",
        symbol="",
        tf="1h",
        force_daily=False,
        merge=True,
    )

    for symbol in symbols:
        binanceDownloader.symbol = symbol
        try:
            binanceDownloader.run()
        except Exception as e:
            print(e)
