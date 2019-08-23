CREATE TABLE floatvals (val FLOAT, color TEXT);
INSERT INTO floatvals (val, color) VALUES (2.34, 'a'), (2.22, 'b'), (2.11, 'c'), (2.44, 'd'), (2.55, 'e');

CREATE TABLE doublevals (val FLOAT8, color TEXT);
INSERT INTO doublevals (val, color) VALUES (2.34, 'a'), (2.22, 'b'), (2.11, 'c'), (2.44, 'd'), (2.55, 'e');

CREATE TABLE sometextvals (name text, id integer);
INSERT INTO sometextvals (name, id) VALUES ('marij', 1), ('chip', 2), ('manny', 3), ('bobby', 4), ('bob', 5);

-- get median for floats - odd count.
SELECT median(val) FROM floatvals;

INSERT INTO floatvals VALUES (2.76, 'f');
-- get median for floats - even count.
SELECT median(val) FROM floatvals;

-- check window function with floats
SELECT val, median(val) OVER (ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM floatvals;

-- get median for doubles
SELECT median(val) FROM doublevals;

INSERT INTO doublevals VALUES (2.76, 'f');
-- get median for doubles - even count.
SELECT median(val) FROM doublevals;

-- check window function with doubles
SELECT val, median(val) OVER (ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM doublevals;

-- median for strings, odd num records.
SELECT * FROM sometextvals ORDER BY name;
SELECT median(name) FROM sometextvals;

insert into sometextvals (name, id) VALUES ('butch', 6);
SELECT * FROM sometextvals ORDER BY name;
SELECT median(name) FROM sometextvals;

-- windowing function for text:
SELECT name, median(name) OVER (ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM sometextvals;


