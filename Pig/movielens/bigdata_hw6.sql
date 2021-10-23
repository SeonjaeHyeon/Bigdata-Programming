-- movies.csv�� ratings.csv�� ���� movie ���̺�� rating ���̺�� ����Ʈ�Ͽ���. (timestamp�� ����)

-- ���� �帣�� ���� ��ȭ�� �ٸ� row�� ����� ó���ϱ� ���� new_movie ���̺� ����
CREATE TABLE new_movie (
  movieid INTEGER,  -- ��ȭ id
  title VARCHAR2(500), -- ��ȭ ����
  genre VARCHAR2(100)  -- ��ȭ �帣
);

/*
�帣 column�� ���ڿ��� ������ '|'�� ������ ���� �ٸ� row�� �и�.
�׸��� ��ȭ id, ��ȭ ����, ���ҵ� �ϳ��� �帣�� ����Ѵ�. �� row���� new_movie ���̺� �߰��Ѵ�.
SQL���� STRSPLITTOBAG, FLATTEN �޼��尡 ���� ���� �˻��� ���� ã�� ����Ʈ���� �ڵ带 ���� �߽��ϴ�.
���� ��ũ: https://lalitkumarb.wordpress.com/2015/03/04/split-comma-delimited-strings-in-a-table-in-oracle/
*/
INSERT INTO new_movie
SELECT movieid, title, trim(regexp_substr(movie.genres, '[^|]+', 1, lines.column_value)) genre
FROM movie, TABLE (CAST (MULTISET
(SELECT LEVEL FROM dual 
      CONNECT BY instr(movie.genres, '|', 1, LEVEL - 1) > 0
      ) AS sys.odciNumberList)) lines;

/*
new_movie ���̺�� rating ���̺��� Natural join�� ��, �帣���� �׷�ȭ�Ѵ�.
�׸��� �帣���� ���� ����(count_rating)�� ���� ���(avg_rating)�� ����Ͽ� �帣�� �Բ� ����Ѵ�.
�̶� ���� ��� �������� ������������ �����Ͽ� ����Ѵ�.
*/
SELECT genre, COUNT(rating) count_rating, AVG(rating) avg_rating
FROM new_movie NATURAL JOIN rating
GROUP BY genre
ORDER BY count_rating DESC;

