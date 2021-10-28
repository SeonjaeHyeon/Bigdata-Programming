# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession

if __name__ == '__main__':
    # WorstMovies라는 이름의 Spark session 생성
    spark = SparkSession.builder.appName('WorstMovies').getOrCreate()

    # ratings.csv, movies.csv 파일을 불러와 각각 DataFrame 데이터로 변수에 저장한다.
    # 헤더 정도는 파라미터에 따라 포함되지 않는다.
    df1 = spark.read.load("hdfs:///user/maria_dev/ml-latest-small/ratings.csv",
                            format='csv', sep=',', inferSchema='true', header='true')
    df2 = spark.read.load("hdfs:///user/maria_dev/ml-latest-small/movies.csv",
                            format='csv', sep=',', inferSchema='true', header='true')

    # Temp view를 생성한다.
    df1.createOrReplaceTempView('ratings')
    df2.createOrReplaceTempView('movies')

    '''
    SQL문을 작성하여 수행한다.
    SELECT title, score, total FROM ... : 영화 제목, 평균 평점, 평점 개수 컬럼을 가져온다.
    movies JOIN r ON movies.movieId = r.movieId : movies 테이블과 r 테이블 (Subquery)을
                                                영화 제목을 기준으로 Inner join한다.
    SELECT movieId, avg(rating) as score, count(rating) as total FROM ratings 
                                            : ratings 테이블에서 영화 id, 평균 평점,
                                            평점 개수 컬럼을 가져온다.
    GROUP BY movieId : 영화 id별로 그룹화 한다.
    WHERE score < 2.0 : 평균 평점이 2.0 미만인 행만 가져온다.
    ORDER BY total DESC : 평점 개수를 기준으로 내림차순 정렬한다.
    LIMIT 30 : 첫 번째 행부터 30개의 행만 가져온다.
    '''
    result = spark.sql("""
        SELECT title, score, total
        FROM movies JOIN (
            SELECT movieId, avg(rating) as score, count(rating) as total
            FROM ratings GROUP BY movieId
        ) r ON movies.movieId = r.movieId
        WHERE score < 2.0
        ORDER BY total DESC
        LIMIT 30
    """)

    '''
    결과를 출력하는 부분이다.
    collect 메서드를 사용하여 DataFrame을 리스트로 만든다.
    for문을 사용하여 리스트의 요소들을 하나씩 뽑아 출력한다.
    '''
    for row in result.collect():
        print(row.title, row.score, row.total)
