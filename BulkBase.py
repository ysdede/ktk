import csv
import os
import platform
import tempfile
from io import BytesIO
from multiprocessing.pool import ThreadPool
from pathlib import Path
from timeit import default_timer as timer
from urllib.request import urlopen
from zipfile import ZipFile

import arrow


class BulkBase:
    def __init__(
        self,
        start: str,
        end: str,
        symbol: str,
        tf: str = "1h",
        market_type: str = "futures",
        margin_type: str = "um",
        data_type: str = "klines",
        worker_count: int = 10,
        force_daily: bool = False,
        merge: bool = False,
    ) -> None:
        """Download kline, premiumIndex, trades data from Binance Vision

        Args:
            start (str): Start date
            end (str): End date
            symbol (str): Dash seperated pair symbol. eg. BTC-USDT
            tf (str, optional): Candle timeframe. Defaults to '1m'.
            market_type (str, optional): Spot or futures. Defaults to 'futures'.
            margin_type (str, optional): Futures only, um, cm (usdt margin, coin margin). Defaults to 'um'.
            data_type (str, optional): Spot: aggTrades, klines, trades
                                        Futures: aggTrades, indexPriceKlines, klines, markPriceKlines,  premiumIndexKlines, trades]. Defaults to 'klines'.
            worker_count (int, optional): Number of download jobs to run paralel. Defaults to 4.
            force_daily (bool, optional): Force daily data. Defaults to False.
        """

        self.timer_start = timer()
        # convert start to datetime object with UTC timezone
        self.start = arrow.get(start).to("UTC")
        self.end = arrow.get(end).to("UTC")
        self.symbol = symbol
        self.sym = None
        self.market_type = market_type

        self.data_type = data_type
        self.margin_type = margin_type
        self.worker_count = worker_count
        self.force_daily = force_daily
        self.mt = (
            None  # combined market type string for usdt margin and coin margin types
        )
        # -> futures/um - futures/cm
        self.tf = tf
        self.period = "monthly"  # monthly, daily
        self.base_url = "https://data.binance.vision/data/"

        self.base_folder = "storage/"  # f'{str(temp_dir)}/bulkdata/'
        os.makedirs(self.base_folder, exist_ok=True)
        print("\033[1m", "\033[37m", "base_folder:", self.base_folder, "\033[0m")

    def csv_files_in_folder(self):  # sourcery skip: avoid-builtin-shadow
        daily_csv = []
        monthly_csv = []

        if not self.force_daily:
            folder = self.get_folder_name("monthly")
            print(folder)
            try:
                monthly_csv = [
                    os.path.join(folder, f)
                    for f in os.listdir(folder)
                    if f.endswith(".csv")
                ]
            except Exception as e:
                print(e)
                monthly_csv = []

        folder = self.get_folder_name("daily")
        print(folder)
        daily_csv = [
            os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".csv")
        ]
        # merge two lists
        all_csv = monthly_csv + daily_csv
        # sort by date
        all_csv.sort()
        return all_csv


    def merge_csv(self, file_list, output):
        import csv

        # Create an empty list to store rows
        rows = []
        header = None

        for file_name in file_list:
            print("Reading file:", file_name)

            # Use csvreader to read the file
            with open(file_name, "r") as csvfile:
                csvreader = csv.reader(csvfile, delimiter=",", quotechar="'")

                for row in csvreader:
                    # Detect header based on the first cell's content, if it's a string andcontains "time" then it's a header
                    if isinstance(row[0], str) and "time" in row[0]:
                        # print("Header found")
                        header = row
                    else:
                        rows.append(row)

        # Sort by date
        print("Sorting rows")
        rows.sort(key=lambda x: x[0])
        # Remove duplicates (optional)
        new_rows = []
        # print("Removing duplicates")
        # for row in rows:
        #     if row not in new_rows:
        #         new_rows.append(row)
        # rows = new_rows

        # Write to the output CSV file
        print("Writing to output file")
        with open(output, "w", newline="") as csvfile:
            csvwriter = csv.writer(csvfile, delimiter=",", quotechar="'")
            
            if not header and self.data_type == 'klines':
                header = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'count', 'taker_buy_volume', 'taker_buy_quote_volume', 'ignore']

            if header:
                csvwriter.writerow(header)  # Write the header at the top
            csvwriter.writerows(rows)  # Write the rows

    def run(self):
        self.sym = self.symbol.replace("-", "")
        # Get list of months since start date

        if not self.force_daily:
            print("Downloading monthly data")
            months = get_months(self.start, self.end)
            self.period = "monthly"
            months_urls, months_checksum_urls = self.make_urls(months)
            self.spawn_downloaders(months_urls)

            # Get this month's days except today
            days = get_days(
                arrow.utcnow().floor("month"),
                arrow.utcnow().floor("day").shift(days=-1),
            )

        else:
            print("Force daily!")
            # Get list of days since start date
            days = get_days(self.start, self.end)

        self.period = "daily"
        days_urls, days_checksum_urls = self.make_urls(days)
        self.spawn_downloaders(days_urls)

        # print(self.csv_files_in_folder())

        output = self.get_output_filename()
        print("\033[94m", "Merging files to", output, "\033[0m")

        self.merge_csv(self.csv_files_in_folder(), output)

    def spawn_downloaders(self, urls):
        # Run multiple threads. Each call will take the next element in urls list
        results = ThreadPool(self.worker_count).imap_unordered(self.download, urls)

        for r, fn in results:  # TODO get rid of this loop
            if r and fn:
                print("\033[92m", "OK", "\033[0m", len(r), fn)
            elif fn:
                print("\033[91m", "FAILED", "\033[0m", r, fn)

    def download(self, url):
        fn, folder_name = self.path_and_fn_from_url(url)
        r_fn = folder_name + fn
        os.makedirs(folder_name, exist_ok=True)

        # download if file doesn't exist
        if not exist(r_fn):
            try:
                print("Downloading", fn)
                print(url)
                http_response = urlopen(url)
                zipfile = ZipFile(BytesIO(http_response.read()))
                zipfile.extractall(path=folder_name)
            except Exception as e:
                print("\033[33m", e, fn, "\033[0m")
                return None, r_fn
        else:
            print("Skipping download", fn, "already exists.")

        if os.stat(r_fn).st_size <= 0:
            return None, r_fn

        # return csv contents as list object
        try:
            with open(r_fn, newline="") as csvfile:
                return list(csv.reader(csvfile, delimiter=",", quotechar="'")), r_fn
        except Exception as e:
            print("\033[33m", e, fn, "\033[0m")
            return None, r_fn

    def get_folder_name(self, period=None):
        if not period or period not in {"monthly", "daily"}:
            period = self.period

        folder_name = (
            f"{self.base_folder}{self.mt}/{period}/{self.data_type}/{self.sym}/"
        )

        if self.data_type not in {"aggTrades", "trades", "metrics"}:
            folder_name += f"{self.tf}/"

        return folder_name

    def get_output_filename(self, period=None):
        if not period or period not in {"monthly", "daily"}:
            period = self.period

        output_fn = f"{self.base_folder}{self.mt}-{self.data_type}-{self.sym}"

        if self.data_type not in {"aggTrades", "trades", "metrics"}:
            output_fn += f"-{self.tf}"

        return f"{output_fn}.csv"

    def path_and_fn_from_url(self, url):
        fn = url.split("/")[-1].replace(".zip", ".csv")

        folder_name = self.get_folder_name()

        return fn, folder_name

    def make_urls(self, date_list):
        urls = []
        checksum_urls = []

        # TODO
        if self.margin_type and self.market_type != "spot":
            self.mt = f"{self.market_type}/{self.margin_type}"
        else:
            self.mt = self.market_type

        for m in date_list:
            if self.data_type in {"aggTrades", "trades", "metrics"}:
                urls.append(
                    # https://data.binance.vision/data/futures/um/daily/metrics/BTCUSDT/BTCUSDT-metrics-2023-08-18.zip
                    f"{self.base_url}{self.mt}/{self.period}/{self.data_type}/{self.sym}/{self.sym}-{self.data_type}-{m}.zip"
                )
            else:
                urls.append(
                    # https://data.binance.vision/data/futures/um/monthly/klines/1INCHUSDT/3m/1INCHUSDT-3m-2023-07.zip
                    f"{self.base_url}{self.mt}/{self.period}/{self.data_type}/{self.sym}/{self.tf}/{self.sym}-{self.tf}-{m}.zip"
                )

            checksum_urls.append(f"{urls[-1]}.CHECKSUM")
        # print(urls)
        return urls, checksum_urls


def get_months(start, end) -> list:
    return [m.strftime("%Y-%m") for m in arrow.Arrow.range("month", start, end)]


def get_days(start, end) -> list:
    return [d.strftime("%Y-%m-%d") for d in arrow.Arrow.range("day", start, end)]


def exist(file_name):
    # TODO Suggest better name
    return os.path.exists(file_name) or os.path.isfile(file_name)
