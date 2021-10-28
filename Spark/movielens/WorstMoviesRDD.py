# -*- coding: utf-8 -*-

from pyspark import SparkConf, SparkContext
from itertools import islice
import csv

def load_movies():
    '''
    영화 정보를 가져오는 함수.

    movies.csv 파일을 csv 라이브러리로 파싱하여 영화 id와 영화 제목을 가져온다.
    그리고 영화 id를 key, 영화 제목을 value로 딕셔너리 변수 movies에 저장하여 반환한다.
    '''

    movies = {}
    with open("/home/maria_dev/ml-latest-small/movies.csv", 'rb') as f:
        reader = csv.reader(f, delimiter=',')
        next(reader)
        for row in reader:
            movies[int(row[0])] = row[1]
    return movies

def parse_input(line):
    '''
    평점 정보를 파싱하는 함수.

    ratings.csv의 각 행을 변수 line으로 받아 파싱하여 영화 id와 영화 평점을 가져온다.
    그리고 영화 id (key), 영화 평점과 1.0의 튜플 (value)을 새로운 튜플로 만들어 반환한다. 
    '''

    fields = line.split(',')
    return (int(fields[1]), (float(fields[2]), 1.0))

if __name__ == '__main__':
    movies = load_movies()  # 영화 데이터가 담긴 변수. load_movies() 함수로 데이터를 불러온다.
    path = "hdfs:///user/maria_dev/ml-latest-small/ratings.csv"

    # WorstMovies라는 이름의 Spark context 생성
    conf = SparkConf().setAppName('WorstMovies')
    sc = SparkContext(conf=conf)

    # 영화 평점이 담긴 변수. ratings.csv 파일을 text로 읽어 RDD 데이터로 만든다.
    lines = sc.textFile(path)

    # csv 파일의 첫 번째 행, 즉 헤더 정보를 RDD 데이터에서 제외한다.
    lines = lines.mapPartitionsWithIndex(
        lambda idx, it: islice(it, 1, None) if idx == 0 else it
    )

    # 평점 데이터에 parse_input() 함수를 각각 적용한다. (Map)
    ratings = lines.map(parse_input)

    # 평점 데이터를 key 값인 영화 id로 병합하면서 lambda 함수를 각각 적용한다. (Reduce)
    # lambda 함수의 m1[0] + m2[0], m1[1] + m2[1]는 각각 평점의 합계와 평점의 개수를 계산한다.
    sum_and_counts = ratings.reduceByKey(lambda m1, m2: (m1[0] + m2[0], m1[1] + m2[1]))

    '''
    평점 데이터에 lambda 함수를 각각 적용한다. (Map)
    lambda 함수에서 v[0] / v[1]는 평점의 평균을 계산하고, v[1]은 단순히 평점 개수를
    value 튜플에 포함시킨다.
    '''
    avg_ratings = sum_and_counts.mapValues(lambda v: (v[0] / v[1], v[1]))

    '''
    filter 메서드를 사용하여 평점 데이터에서 lambda 함수 조건을 만족하는 것만 뽑아
    새로운 RDD 데이터를 생성한다.
    lambda 함수에서 x[1][0] < 2.0는 평점의 평균이 2.0 미만인 경우이다.
    '''
    filtered_ratings = avg_ratings.filter(lambda x: x[1][0] < 2.0)

    # 새로운 평점 데이터를 sortBy 메서드를 사용하여 lambda 함수 조건을 기준으로 내림차순 정렬한다.
    # lambda 함수에서 x[1][1]은 평점 개수이다.
    sorted_movies = filtered_ratings.sortBy(lambda x: x[1][1], ascending=False)

    # take 메서드를 사용하여 평점 데이터에서 첫 번째부터 30개만 뽑아 리스트로 반환한다.
    # 그리고 변수 results에 저장한다.
    results = sorted_movies.take(30)

    # 결과를 출력하는 부분이다.
    # for문을 사용하여 리스트의 요소들을 하나씩 뽑아 출력한다.
    for key, value in results:
        print(movies[key], value[0], value[1])
