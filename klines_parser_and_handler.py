import numpy as np
import requests
import urllib.request
import os
import zipfile
from enum import unique, Enum
import time
import pandas as pd
import re

pd.set_option('display.max_colwidth', None)


@unique
class TimeFrames(Enum):
    SECOND_1 = '1s'
    SECOND_30 = '30s'
    MINUTE_1 = '1m'
    MINUTE_3 = '3m'
    MINUTE_5 = '5m'
    MINUTE_15 = '15m'
    MINUTE_30 = '30m'
    HOUR_1 = '1h'

    @staticmethod
    def get_divider(timeframe):
        if timeframe == TimeFrames.SECOND_1:
            return 1000
        if timeframe == TimeFrames.SECOND_30:
            return 1000 * 30
        elif timeframe == TimeFrames.MINUTE_1:
            return 1000 * 60
        elif timeframe == TimeFrames.MINUTE_3:
            return 1000 * 60 * 3
        elif timeframe == TimeFrames.MINUTE_5:
            return 1000 * 60 * 5
        elif timeframe == TimeFrames.MINUTE_15:
            return 1000 * 60 * 15
        elif timeframe == TimeFrames.MINUTE_30:
            return 1000 * 60 * 30
        elif timeframe == TimeFrames.HOUR_1:
            return 1000 * 60 * 60


@unique
class TypeOfTool(Enum):
    """
    Выбери тип инструмента, откуда собираешься скачивать сделки:
    либо "spot",
    либо "futures"(здесь также нужно выбрать тип фьючерса: "um" или "cm").
    """
    SPOT = "spot"
    FUTURES_UM = "futures/um"
    FUTURES_CM = "futures/cm"


def file_download(tool: str, type_of_tool: TypeOfTool, amount_of_files, timeframe: TimeFrames = TimeFrames.MINUTE_1):
    """
    Скачивает список сделок в формате csv в формате - один месяц-один файл.

    :param tool: Название инструмента, как указано в Binance в формате str, например "BTCUSDT".
     ОСНОВНЫЕ НАЗВАНИЯ: "SOLUSDT", "XLMUSDT", "THETAUSDT", "EOSUSDT", "BTCUSDT", "DOGEUSDT", "ETHUSDT"
    :param type_of_tool: описание в TypeOfTool.
    :param timeframe: Таймфрейм в str формате, на основе которого и будет обработаны и записаны данные.
     Может принимать значения: 1s 30s 1m 3m 5m 15m 30m 1h .
    :param amount_of_files: количество месяцев, которые надо скачать, может принимать численное значение, а может принять значение "max",
     что значит, что скачиваться будут все данные
    """

    def final_file_creating():
        """
        Функция, которая, обрабатывая некоторые данные, составляет правильное название csv-файла, в который будут сохранены все скачанные данные(если они поместятся.
        """
        end = re.sub("([a-zA-Z]*-[0-9a-zA-Z]*-)", "", filename)
        end = end.replace(".zip", ".csv").replace("-", ".")
        start_replace1 = re.findall("(-[0-9]{4}-[0-9]{2})", start_date)[-1]
        start_replace2 = start_replace1.replace("-", ".")
        start_replace2 = start_replace2.replace(".", "-", 1)
        start = start_date.replace(start_replace1, start_replace2)

        file = "csv_files/" + start + "-" + end
        main_data["time"] = main_data["time"] // 10 ** 3
        main_data.to_csv(file, index=False)
        print("Данные были сохранены в файл", file)

    def vars_checking():
        """Просто проверка правильности ввода переменных."""

        # Простая сверка типов
        if type(tool) != str:
            raise ValueError("tool может принимать только str значения")
        elif type(type_of_tool) != TypeOfTool:
            raise ValueError("type_of_tool может принимать только TypeOfTool значения")
        elif type(amount_of_files) != int and amount_of_files != "max":
            raise ValueError("amount_of_files может принимать только int значения или же значение 'max'")
        elif type(timeframe) != TimeFrames:
            raise ValueError("timeframe может принимать только TimeFrames значения")
        elif type(type_of_data) != str:
            raise ValueError("type_of_data может принимать только str значения")
        elif type_of_data != "klines":
            raise ValueError("type_of_data должно иметь значение klines!")

    vars_checking()
    type_of_data = "klines"

    url = f'https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix=data/{type_of_tool.value}/monthly/{type_of_data}/{tool}/'

    url += timeframe.value + "/"
    start_index, end_index, url_file, filename = 0, 0, "CHECKSUM", "CHECKSUMCHECKSUMCHECKSUM"
    start_date = ""

    response = requests.get(url)
    response_text = response.text
    if amount_of_files == "max" and type_of_data == "klines":
        amount_of_files = 1000000
    main_data = pd.DataFrame(columns=[
        "time", "open", "high", "low", "close"
    ])

    for i in range(amount_of_files):
        url_file = "CHECKSUM"
        while "CHECKSUM" in url_file:
            start_index = response_text.find("<Key>", end_index) + 5
            if start_index < end_index:
                final_file_creating()
                return
            end_index = response_text.find(".zip", start_index) + 4
            url_file = response_text[start_index:end_index]
            if filename in url_file:
                url_file = "CHECKSUM"

        if url_file == "" or "</ListBucketResult>" in url_file:
            final_file_creating()
            return
        filename = url_file.find(f"/{tool}-") + 1
        filename = url_file[filename:]
        if start_date == "":
            start_date = filename.replace(".zip", "")
        csv_path = "csv_files/" + filename.replace("zip", "csv")
        print(filename)

        if not os.path.exists("csv_files/"):
            os.mkdir("csv_files/")
        urllib.request.urlretrieve("https://data.binance.vision/" + url_file, "csv_files/" + filename)

        with zipfile.ZipFile("csv_files/" + filename) as csv_zip:
            csv_zip.extractall("csv_files/")

        with open(csv_path, "r") as file:
            if "open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore" not in file.readline():
                flag = True
            else:
                flag = False
        if flag:
            with open(csv_path, "r") as file:
                file_read = "open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore\n" + file.read()
            with open(csv_path, "w") as file:
                file.write(file_read)

        main2 = pd.read_csv(csv_path, dtype={"open_time": np.int64})
        main2 = main2.rename(columns={"open_time": "time"})
        main2 = main2[["time", "open", "high", "low", "close"]]
        main_data = pd.concat([main_data, main2], ignore_index=True)
        os.remove(csv_path)

        os.remove("csv_files/" + filename)
    final_file_creating()


if __name__ == "__main__":
    start_time = time.time()
    file_download("XLMUSDT", TypeOfTool.FUTURES_UM, "max", TimeFrames.HOUR_1)
    print("Время полной закачки выбранного количества файлов =", time.time() - start_time)
