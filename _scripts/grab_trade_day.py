import requests
import json
import requests.packages.urllib3.util.ssl_
import time

requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = "ALL"  # 防止SSL限制

years = [2020, 2021, 2022, 2023, 2024]


def save_trading_date(date_str: str, is_trading_day: bool):
    """保存结果：交易日/非交易日
    :param date_str: 日期，字符串格式
    :param is_trading_day: 是否为交易日，为了区分交易日保存
    """
    if is_trading_day:
        save_file_path = "trade_day.txt"
    else:
        save_file_path = "not_trade_day.txt"
    with open(save_file_path, "a") as file:
        file.write(date_str)
        file.write("\n")


def get_trading_date(month_date: str):
    """
    :param month_date: 日期，例 2020-01、2022-12
    """
    target_url = "http://www.szse.cn/api/report/exchange/onepersistenthour/monthList?month={}".format(
        month_date
    )
    send_headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36",
        "Connection": "keep-alive",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    }
    time.sleep(2)  # 限制频率
    req = requests.get(target_url, headers=send_headers)
    json_state = json.loads(req.text)
    for dict_value in json_state["data"]:
        print(dict_value)
        if dict_value["jybz"] == "0":  # 非交易日
            save_trading_date(dict_value["jyrq"], False)
        elif dict_value["jybz"] == "1":  # 交易日
            save_trading_date(dict_value["jyrq"], True)


def main():
    for _year_i in list(map(lambda x: str(x), years)):
        for _month_i in range(12):
            get_trading_date("{}-{}".format(_year_i, _month_i + 1))


if __name__ == "__main__":
    main()
