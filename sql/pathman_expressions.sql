\set VERBOSITY terse

SET search_path = 'public';
CREATE SCHEMA pathman;
CREATE EXTENSION pg_pathman SCHEMA pathman;
CREATE SCHEMA test;

/* hash */
CREATE TABLE test.hash_rel (
	id		SERIAL PRIMARY KEY,
	value	INTEGER,
	value2  INTEGER
);
INSERT INTO test.hash_rel (value, value2)
	SELECT val, val * 2 FROM generate_series(1, 5) val;

SELECT COUNT(*) FROM test.hash_rel;
SELECT pathman.create_hash_partitions('test.hash_rel', 'value * value2', 4);
SELECT COUNT(*) FROM ONLY test.hash_rel;
SELECT COUNT(*) FROM test.hash_rel;

INSERT INTO test.hash_rel (value, value2)
	SELECT val, val * 2 FROM generate_series(6, 10) val;
SELECT COUNT(*) FROM ONLY test.hash_rel;
SELECT COUNT(*) FROM test.hash_rel;

EXPLAIN (COSTS OFF) SELECT * FROM test.hash_rel WHERE value = 5;
EXPLAIN (COSTS OFF) SELECT * FROM test.hash_rel WHERE (value * value2) = 5;

/* range */
CREATE TABLE test.range_rel (
	id	SERIAL PRIMARY KEY,
	dt	TIMESTAMP,
	txt	TEXT);

INSERT INTO test.range_rel (dt, txt)
SELECT g, md5(g::TEXT) FROM generate_series('2015-01-01', '2020-04-30', '1 month'::interval) as g;
SELECT pathman.create_range_partitions('test.range_rel', 'AGE(dt, ''2000-01-01''::DATE)',
		'15 years'::INTERVAL, '1 year'::INTERVAL, 10);
INSERT INTO test.range_rel_1 (dt, txt) VALUES ('2020-01-01'::DATE, md5('asdf'));
SELECT * FROM test.range_rel_6;
INSERT INTO test.range_rel_6 (dt, txt) VALUES ('2020-01-01'::DATE, md5('asdf'));

