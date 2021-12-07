import os
import time
import xml.etree.ElementTree as ET

import requests

now = time.localtime()

API_KEY = "5558754a4e68736a39357268625472"
# API_ENDPOINT = f"http://openapi.seoul.go.kr:8088/{API_KEY}/xml/SPOP_LOCAL_RESD_JACHI/1/1000/{now.tm_year}{now.tm_mon}{now.tm_mday}"
API_ENDPOINT = f"http://openapi.seoul.go.kr:8088/{API_KEY}/xml/SPOP_LOCAL_RESD_JACHI/1/1000/20211110"

session = requests.Session()

response = None
while True:
    response = session.get(API_ENDPOINT)
    if response.status_code == 200:
        break
    else:
        print("Could not get data. Retrying in 30 seconds.")
        time.sleep(30)

dir_name = os.path.join(os.getcwd(), "seoul_data")
if not os.path.isdir(dir_name):
    os.mkdir(dir_name)

with open(f"{dir_name}/{time.strftime('%y%m%d', now)}.xml", "w", encoding="utf-8") as file:
    file.write(response.text)

# root = ET.fromstring(response.text)

# total = root.findtext('list_total_count')
# print(f"{total=}")

# rows = root.findall("row")
# for row in rows:
#     print(row)
