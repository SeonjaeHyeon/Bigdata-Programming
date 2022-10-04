Pig Latin을 이용하여  
영화 장르별 평점 회수(count)와 평균 평점을 count의 역순으로 출력하시오.  
(pig의 경우 결과를 best_genre라는 파일에 저장하시오.)

- 전략: 위 문제를 풀기 위한 SQL문을 작성해보시오.  
- PigLatin 코드: 위 문제를 풀기위한 pig 코드를 작성하시오.  

**제출내용**  
- SQL 코드와 전략 설명  
- Pig Latin 코드 with comment  
- 실행 방법과 예제  

**채점 기준**  
- SQL: 30점  
- PigLatin:50점  
- 설명과 보고서: 20점  

**힌트**  
- 장르는 movies.csv에 들어있음.  
- 하나의 영화에 여러개의 장르가 들어있을 수 있음. 이를 여러 라인으로 만들어 처리 하기 위해 STRSPLITTOBAG과 FLATTEN을 사용할 수 있음 (https://stackoverflow.com/questions/19580442/using-tobag-and-strsplit-in-pig-latin)  
- CSV 파일을 파싱할 때는 'org.apache.pig.piggybank.storage.CSVExcelStorage'를 사용  
