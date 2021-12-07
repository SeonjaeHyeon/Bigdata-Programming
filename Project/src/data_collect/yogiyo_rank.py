import os
import time
import requests

URL_ENDPOINT = "https://www.yogiyo.co.kr/api/v1/restaurants-geo/"
REVIEW_ENDPOINT = "https://www.yogiyo.co.kr/api/v1/reviews/{}/"
ORDER_BY = ["rank", "review_count"]
COORDINATES = {
    "강남구": ["37.5175697781766", "127.047486426456"],  # 강남구청
    "강동구": ["37.5299385098609", "127.124030864102"],  # 강동구청
    "강북구": ["37.6395554560466", "127.025477035178"],  # 강북구청
    "강서구": ["37.5508963767587", "126.849674642518"],  # 강서구청
    "관악구": ["37.4782623042655", "126.951546468884"],  # 관악구청
    "광진구": ["37.5384212800209", "127.08244466099"],  # 광진구청
    "구로구": ["37.4952932350333", "126.887650564479"],  # 구로구청
    "금천구": ["37.4568212805241", "126.895439190191"],  # 금천구청
    "노원구": ["37.6541632798257", "127.05667609023"],  # 노원구청
    "도봉구": ["37.6687161303354", "127.047131407553"],  # 도봉구청
    "동대문구": ["37.57439171481", "127.039896531392"],  # 동대문구청
    "동작구": ["37.5122931661255", "126.939466317838"],  # 동작구청
    "마포구": ["37.5663629539472", "126.901613080465"],  # 마포구청
    "서대문구": ["37.579177798914", "126.93659820708"],  # 서대문구청
    "서초구": ["37.4837423289271", "127.032589331572"],  # 서초구청
    "성동구": ["37.5634225124879", "127.036964954734"],  # 성동구청
    "성북구": ["37.5894562526765", "127.016849541939"],  # 성북구청
    "송파구": ["37.5145970992079", "127.106078741252"],  # 송파구청
    "양천구": ["37.517075413496", "126.866542566513"],  # 양천구청
    "영등포구": ["37.5262632307561", "126.895957515141"],  # 영등포구청
    "용산구": ["37.5324523203077", "126.990478777654"],  # 용산구청
    "은평구": ["37.6025832474542", "126.929392328342"],  # 은평구청
    "종로구": ["37.5735051388208", "126.978988241377"],  # 종로구청
    "중구": ["37.5637508932052", "126.997820175213"],  # 중구청
    "중랑구": ["37.6066456570634", "127.092889491438"],  # 중랑구청
}

HTTP_HEADER = {
    "x-apikey": "iphoneap",
    "x-apisecret": "fe5183cc3dea12bd0ce299cf110a75a2",
    "Referer": "https://www.yogiyo.co.kr/mobile/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36",
}


session = requests.Session()

for region, coordinate in COORDINATES.items():
    query = {
        "items": "80",
        "lat": coordinate[0],
        "lng": coordinate[1],
        "order": ORDER_BY[0],
        "page": "0",
        "search": "",
    }

    json_data = None
    while True:
        response = session.get(URL_ENDPOINT, headers=HTTP_HEADER, params=query)
        try:
            json_data = response.json()
            break
        except:
            print("Could not get data. Retrying in 15 seconds.")
            time.sleep(15)

    now = time.localtime()
    dir_name = f"{time.strftime('%y%m%d-%H', now)}"
    if not os.path.isdir(dir_name):
        os.mkdir(dir_name)
    with open(f"{dir_name}/rank-{region}.json", "w", encoding="utf-8") as file:
        file.write(response.text)

    # restaurants = json_data.get("restaurants", {})
    # for restaurant in restaurants:
    #     shop_id = restaurant.get("id")

    #     if shop_id is None:
    #         print("Shop id is None. Skipping.")
    #         continue

    #     query2 = {
    #         "count": "40",
    #         "only_photo_review": "false",
    #         "page": "1",
    #         "sort": "time",
    #     }

    #     while True:
    #         response2 = session.get(REVIEW_ENDPOINT.format(shop_id), headers=HTTP_HEADER, params=query2)
    #         try:
    #             json_data2 = response2.json()
    #             break
    #         except:
    #             print("Could not get review data. Retrying in 15 seconds.")
    #             time.sleep(15)

    #     dir_name2 = os.path.join(dir_name, "review")
    #     if not os.path.isdir(dir_name2):
    #         os.mkdir(dir_name2)
    #     dir_name3 = os.path.join(dir_name2, region)
    #     if not os.path.isdir(dir_name3):
    #         os.mkdir(dir_name3)
    #     with open(f"{dir_name3}/review-{shop_id}.json", "w", encoding="utf-8") as file:
    #         file.write(response2.text)

    #     time.sleep(1.5)

    time.sleep(2)
