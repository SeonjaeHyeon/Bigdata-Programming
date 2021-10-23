-- movies.csv와 ratings.csv는 각각 movie 테이블과 rating 테이블로 임포트하였다. (timestamp는 생략)

-- 여러 장르를 가진 영화를 다른 row로 만들어 처리하기 위한 new_movie 테이블 생성
CREATE TABLE new_movie (
  movieid INTEGER,  -- 영화 id
  title VARCHAR2(500), -- 영화 제목
  genre VARCHAR2(100)  -- 영화 장르
);

/*
장르 column의 문자열을 구분자 '|'로 나누고 각각 다른 row로 분리.
그리고 영화 id, 영화 제목, 분할된 하나의 장르를 출력한다. 이 row들을 new_movie 테이블에 추가한다.
SQL에서 STRSPLITTOBAG, FLATTEN 메서드가 없어 구글 검색을 통해 찾은 사이트에서 코드를 참고 했습니다.
참고 링크: https://lalitkumarb.wordpress.com/2015/03/04/split-comma-delimited-strings-in-a-table-in-oracle/
*/
INSERT INTO new_movie
SELECT movieid, title, trim(regexp_substr(movie.genres, '[^|]+', 1, lines.column_value)) genre
FROM movie, TABLE (CAST (MULTISET
(SELECT LEVEL FROM dual 
      CONNECT BY instr(movie.genres, '|', 1, LEVEL - 1) > 0
      ) AS sys.odciNumberList)) lines;

/*
new_movie 테이블과 rating 테이블을 Natural join한 뒤, 장르별로 그룹화한다.
그리고 장르별로 평점 개수(count_rating)와 평점 평균(avg_rating)을 계산하여 장르와 함께 출력한다.
이때 평점 평균 기준으로 내림차순으로 정렬하여 출력한다.
*/
SELECT genre, COUNT(rating) count_rating, AVG(rating) avg_rating
FROM new_movie NATURAL JOIN rating
GROUP BY genre
ORDER BY count_rating DESC;

