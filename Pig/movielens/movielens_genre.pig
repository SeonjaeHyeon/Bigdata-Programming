-- LOAD를 사용하여 데이터를 로드한다.
-- CSVExcelStorage() 메서드를 사용하여, 첫 번째 row(컬럼명이 표기된 헤더 row)를 제외하고 csv 파일의 데이터를 로드한다.
-- movies.csv 데이터 로드 (영화id, 영화 제목, 영화 장르)
movies = LOAD '/user/maria_dev/ml-latest-small/movies.csv'
			USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER')
            AS (movieId:int, title:chararray, genres:chararray);
-- ratings.csv 데이터 로드 (유저id, 영화id, 평점, 타임스탬프)
ratings = LOAD '/user/maria_dev/ml-latest-small/ratings.csv'
			USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER')
            AS (userId:int, movieId:int, rating:float, timestamp:long);

/*
FOREACH를 사용하여 movies 데이터를 한 줄(row)씩 가져온다.
그리고 GENERATE로 영화 id, 영화 제목, 분할된 하나의 장르(genre) 정보를 가진 row를 만들어 new_movies에 추가한다.
구분자 '|'로 구분된 여러 개의 영화 장르를 분할하기 위해 FLATTEN(), STRSPLITTOBAG() 메서드를 사용하였다.
STRSPLITTOBAG() 메서드는 문자열을 구분자로 분할하여 하나의 bag으로 반환한다. FLATTEN() 메서드는 이를 풀어 tuple로 반환한다.
*/
new_movies = FOREACH movies GENERATE movieId, title, FLATTEN(STRSPLITTOBAG(genres, '\\|')) AS genre;

-- JOIN BY를 사용하여 new_movies와 ratings를 movieId를 기준으로 Join 한다. 
-- 따라서 new_movies의 영화 id와 ratings의 영화 id가 같은 것끼리 합쳐진다.
joined = JOIN new_movies BY movieId, ratings BY movieId;
-- GROUP BY를 사용하여 joined 데이터를 장르별로 그룹화한다.
grouped = GROUP joined BY genre;

/*
FOREACH를 사용하여 grouped 데이터를 한 줄씩 가져온다.
그리고 GENERATE로 장르, 해당 장르의 평점 개수, 해당 장르의 평점 평균 정보를 가진 row를 만들어 aggregated에 추가한다.
평점의 개수를 세기 위해 COUNT(), 평점의 평균을 계산하기 위해 AVG() 메서드를 사용하였다.
*/
aggregated = FOREACH grouped GENERATE $0 AS genre, COUNT(joined.rating) AS count_rating, AVG(joined.rating) AS avg_rating;
-- ORDER BY를 사용하여 aggreagted 데이터를 평점의 개수를 기준으로 내림차순(DESC) 정렬한다.
sorted = ORDER aggregated BY count_rating DESC;

-- DUMP를 사용하여 최종 결과를 출력한다.
DUMP sorted;
-- STORE INTO를 사용하여 최종 결과를 파일로 저장한다.
-- 이때 CSVExcelStorage() 메서드를 사용하여, 첫 번째 row에 헤더명을 포함하고 csv 파일로 저장한다.
STORE sorted INTO '/user/maria_dev/pig-output/best_genre'
					USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'WRITE_OUTPUT_HEADER');
