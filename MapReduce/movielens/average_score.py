# -*- coding: utf-8 -*-
# python 2.7은 기본 인코딩이 utf-8이 아니므로 utf-8로 설정

import csv  # csv 파일 파싱을 위한 라이브러리

from mrjob.job import MRJob
from mrjob.step import MRStep

def get_average(gen):
    '''
    평균을 계산하는 함수.

    제너레이터를 파라미터로 받아 for문에서 제너레이터의 값을 하나씩 꺼낸다.
    for문이 반복될 때마다 count가 1씩 증가하고, value에는 제너레이터의 값이 누적해서 저장된다.
    그런 다음 value에서 count를 나누어 평균을 계산한 뒤 반환한다.

    count는 int형, value는 float형 변수이다.
    '''

    count = 0
    value = 0.0

    for item in gen:
        count += 1
        value += item

    # value가 float형이므로 나눗셈 결과 또한 float형이다.
    # count가 0인 경우는 없으므로 ZeroDivisionError는 일어나지 않는다.
    return value / count 


class AverageScore(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.map_preprocess,
                    reducer=self.reduce_preprocess),
            MRStep(reducer=self.reduce_rating_count),
            MRStep(reducer=self.reduce_sort)
        ]

    def map_preprocess(self, _, line):
        '''
        ratings.csv의 데이터와 movies.csv의 데이터의 Join을 위한 전처리 과정을 수행하는 mapper 클래스.

        csv 데이터를 파싱하고 두 가지 파일 모두 영화 id를 key 값으로 설정하여 value값과 함께 반환.
        ratings.csv에서 필요한 정보는 영화 id, 영화 평점이고 movies.csv에서 필요한 정보는 영화 id, 영화 제목이다.
        '''

        data = next(csv.reader([line], delimiter=',', quotechar='"'))  # csv 데이터 파싱
        if data[0] not in ('userId', 'movieId'):  # csv 파일의 첫 번째 행(컬럼명이 적힌 행)은 데이터로 사용하지 않음
            if len(data) == 4:  # rating.csv는 컬럼 수가 4개이다.
                # data[1]: movieId, data[2]: rating
                # 영화 id, 영화 평점 반환
                yield data[1], float(data[2])
            else:  # movies.csv는 컬럼 수가 3개이다.
                # data[0]: movieId, data[1]: title 반환
                # 영화 id, 영화 제목 반환
                yield data[0], data[1]

    def reduce_preprocess(self, _, values):
        '''
        mapper 클래스에서 파싱 후 Join을 수행한 데이터에서 영화 제목을 가져오고,
        리뷰가 10개 이하인 영화를 제외하는 reducer 클래스.
        '''

        data = []  # 데이터를 저장하기 위한 리스트

        # 영화 제목을 저장하기 위한 문자열 변수
        # movies.csv에 정보가 없는 영화가 존재할 수 있으므로, Unknown Movie로 초기화.
        # 따라서 제목을 알 수 없는 영화의 리뷰는 모두 Unknown Movie으로 계산된다.
        title = 'Unknown Movie'

        for value in values:  # 영화 평점들이 담긴 제너레이터를 for문으로 하나씩 뽑아 value에 저장
            # str형을 float형으로 변환 시 ValueError를 발생시키고 catch문으로 넘어감.
            # python 2.7 호환을 위해 부득이하게 일부러 ValueError를 발생시켜 처리함.
            try:
                float(value)  # value를 float형으로 변환.
                data.append(value)  # 예외가 발생하지 않았을 경우 영화 평점 데이터이므로 data에 추가
            except:  # ValueError가 발생한 경우
                title = value  # str형 데이터는 영화 제목이므로 title에 저장

        if len(data) > 10:  # 리뷰 횟수가 10개 보다 많은 경우
            for item in data:  # 영화 평점이 담긴 리스트 데이터인 data에서 하나씩 뽑아 item에 저장
                yield title, item  # 영화 제목, 영화 평점 반환

    def reduce_rating_count(self, key, values):
        '''
        영화의 평균 평점을 계산하기 위한 reducer 클래스.
        
        메서드로 정의한 get_average() 함수에 영화 평점 데이터가 담긴 제너레이터를 파라미터로 넘겨서 나온 결과를 avg에 저장
        '''

        avg = get_average(values)  # 영화 평균 평점 계산
        # 영화 평균 평점과 영화 id 반환.
        # 다음 step에서 정렬을 위해 key는 모두 None, value는 영화 평균 평점과 영화 제목을 담은 튜플을 반환
        yield None, (avg, key) 

    def reduce_sort(self, _, values):
        '''
        내림차순 정렬을 위한 reducer 클래스.

        내림차순 정렬을 위해 sorted() 메서드를 이용하였다.
        '''

        # float형인 평균 평점을 기준으로, reverse=True로 내림차순 정렬.
        # values는 영화 평균 평점과 영화 id를 담은 튜플이므로 sorted() 메서드로 정렬 후,
        # for문으로 하나씩 뽑아 average와 title에 저장
        for average, title in sorted(values, reverse=True):
            # 영화 제목과 평균 평점 반환. (최종 결과)
            yield title, str(average)


if __name__ == '__main__':
    AverageScore.run()
