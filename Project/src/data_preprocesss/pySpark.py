# -*- coding: utf-8 -*-

from pyspark import SparkConf, SparkContext

from itertools import islice
import csv
import re
import os.path

LOCAL_PATH = '/home/maria_dev/bigpro'
PATH = 'hdfs:///user/maria_dev/bigpro'

def load_dates():
    dates = []
    with open(os.path.join(LOCAL_PATH, 'yogiyo/yogiyo-강남구.csv'), 'r') as f:
        reader = csv.reader(f, delimiter=',')
        next(reader)
        for row in reader:
            date = row[2]
            if date not in dates:
                dates.append(row[-2])

    return dates

def load_regions():
    regions = []

    regex = re.compile('yogiyo-(.*).csv')

    for (_, _, files) in os.walk(os.path.join(LOCAL_PATH, 'yogiyo')):
        for file in files:
            regions.append(regex.findall(file)[0])

    return regions

def parse_input(line):
    fields = next(csv.reader([line.encode('utf-8')], delimiter=',', quotechar='"'))
    # 가게id (key), 가게 평점, 가게 순위, 1 (value)
    return (int(fields[1]), (float(fields[3]), int(fields[4]), 1.0))

if __name__ == '__main__':
    dates = load_dates()
    regions = load_regions()

    csv_file = open('result.csv', 'wb')
    csv_writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    csv_writer.writerow(['id', 'review_avg', 'rank', 'count', 'region'])

    for i, region in enumerate(regions):
        conf = SparkConf().setAppName('Yogiyo-' + str(i))
        sc = SparkContext(conf=conf)

        lines = sc.textFile(os.path.join(PATH, 'yogiyo', 'yogiyo-' + region + '.csv'))

        lines = lines.mapPartitionsWithIndex(
            lambda idx, it: islice(it, 1, None) if idx == 0 else it
        )

        shops = lines.map(parse_input)

        sum_and_counts = shops.reduceByKey(lambda m1, m2: (m1[0] + m2[0], m1[1] + m2[1], m1[2] + m2[2]))

        avg_ratings = sum_and_counts.mapValues(lambda v: (v[0] / v[2], v[1] / v[2], v[2]))

        # 평균 순위 기준으로 정렬
        sorted_shops = avg_ratings.sortBy(lambda x: x[1][2], ascending=False)

        results = sorted_shops.take(100)

        for key, value in results:
            csv_writer.writerow([key, value[0], value[1], value[2], i])

        sc.stop()

    csv_file.close()


