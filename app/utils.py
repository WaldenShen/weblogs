#!/usr/bin/python
# coding=UTF-8

import re

SEP = "\t"
NEXT = ">"
ENCODE_UTF8 = "UTF-8"

def load_category(filepath):
    results = {}

    '''
    平台    產品1   產品2   Function    Intention   landing後的URL
    官網    財管    財管    理財    理財投資    https://www.cathayholdings.com/bank/cathaybk/vip/myvip_4.asp
    官網    財管    財管    理財    理財投資    https://www.cathayholdings.com/bank/cathaybk/vip/myvip_4_01.asp
    官網    財管    財管    理財    理財投資    https://www.cathayholdings.com/bank/Cathaybk/vip/myvip_4_02_01.asp
    官網    基金    基金    理財    理財投資    https://www.cathayholdings.com/bank/cathaybk/vip/myvip_4_02_02.asp
    官網    個人信託    個人信託    理財    理財投資    https://www.cathayholdings.com/bank/cathaybk/vip/myvip_4_02_03.asp
    官網    財管    財管    理財    理財投資    https://www.cathayholdings.com/bank/Cathaybk/vip/myvip_4_03_01.asp
    官網    財管    財管    理財    理財投資    https://www.cathayholdings.com/bank/cathaybk/vip/myvip_index.asp
    官網    財管    財管    產品    理財投資    https://www.cathayholdings.com/bank/cathaybk/vip/myvip_news1030820.asp
    ...
    ...
    ...
    '''
    with open(filepath, "r", encoding="UTF8") as in_file:
        is_header = True

        for line in in_file:
            if is_header:
                is_header = False
            else:
                info = re.split(",", line.strip().lower())

                website, product_1, product_2, function, intention, url = info
                results.setdefault(url, {"logic": "{}_{}".format(product_1, product_2), "function": function, "intention": intention})

    return results

def norm_url(url):
    start_idx = url.find("?")
    return url[:start_idx if start_idx > -1 else len(url)]

def get_date_type(filename):
    date = filename.split("_")[2].split(".")[0]

    date_type = None
    if len(date) == 10:
        date_type = "day"
    elif len(date) == 4:
        date_type = "year"
    elif date.upper().find("W") > -1:
        date_type = "week"
    else:
        date_type = "month"

    return date_type

if __name__ == "__main__":
    category = load_category("../data/setting/category.tsv")
    for url, c in category.items():
        for f, value in c.items():
            print(url, f, value)
