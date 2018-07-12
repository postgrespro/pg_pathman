/*
 * -------------------------------------------
 *  NOTE: This test behaves differenly on < 11 because planner now turns
 *  Row(Const, Const) into just Const of record type, apparently since 3decd150
 * -------------------------------------------
 */

\set VERBOSITY terse

SET search_path = 'public';
CREATE EXTENSION pg_pathman;
CREATE SCHEMA test_exprs;



/*
 * Test partitioning expression canonicalization process
 */

CREATE TABLE test_exprs.canon(c JSONB NOT NULL);
SELECT create_range_partitions('test_exprs.canon', '(C->>''key'')::int8', 1, 10, 2);
SELECT expr FROM pathman_config; /* check expression */
INSERT INTO test_exprs.canon VALUES ('{ "key": 2, "value": 0 }');
SELECT *, tableoid::REGCLASS FROM test_exprs.canon;
DROP TABLE test_exprs.canon CASCADE;


CREATE TABLE test_exprs.canon(val TEXT NOT NULL);
CREATE SEQUENCE test_exprs.canon_seq;
SELECT add_to_pathman_config('test_exprs.canon', 'VAL collate "C"', NULL);
SELECT add_range_partition('test_exprs.canon', 'a'::TEXT, 'b');
SELECT add_range_partition('test_exprs.canon', 'b'::TEXT, 'c');
SELECT add_range_partition('test_exprs.canon', 'c'::TEXT, 'd');
SELECT add_range_partition('test_exprs.canon', 'd'::TEXT, 'e');
SELECT expr FROM pathman_config; /* check expression */
INSERT INTO test_exprs.canon VALUES ('b');
SELECT *, tableoid::REGCLASS FROM test_exprs.canon;
EXPLAIN (COSTS OFF) SELECT * FROM test_exprs.canon WHERE val COLLATE "C" < ALL (array['b', 'c']);
EXPLAIN (COSTS OFF) SELECT * FROM test_exprs.canon WHERE val COLLATE "POSIX" < ALL (array['b', 'c']);
DROP TABLE test_exprs.canon CASCADE;



/*
 * Test composite key.
 */
CREATE TABLE test_exprs.composite(a INT4 NOT NULL, b TEXT NOT NULL);
CREATE SEQUENCE test_exprs.composite_seq;
SELECT add_to_pathman_config('test_exprs.composite',
							 '(a, b)::test_exprs.composite',
							 NULL);
SELECT add_range_partition('test_exprs.composite',
						   '(1,a)'::test_exprs.composite,
						   '(10,a)'::test_exprs.composite);
SELECT add_range_partition('test_exprs.composite',
						   '(10,a)'::test_exprs.composite,
						   '(20,a)'::test_exprs.composite);
SELECT add_range_partition('test_exprs.composite',
						   '(20,a)'::test_exprs.composite,
						   '(30,a)'::test_exprs.composite);
SELECT add_range_partition('test_exprs.composite',
						   '(30,a)'::test_exprs.composite,
						   '(40,a)'::test_exprs.composite);
SELECT expr FROM pathman_config; /* check expression */
INSERT INTO test_exprs.composite VALUES(2,  'a');
INSERT INTO test_exprs.composite VALUES(11, 'a');
INSERT INTO test_exprs.composite VALUES(2,  'b');
INSERT INTO test_exprs.composite VALUES(50, 'b');
SELECT *, tableoid::REGCLASS FROM test_exprs.composite;
EXPLAIN (COSTS OFF) SELECT * FROM test_exprs.composite WHERE (a, b)::test_exprs.composite < (21, 0)::test_exprs.composite;
EXPLAIN (COSTS OFF) SELECT * FROM test_exprs.composite WHERE (a, b) < (21, 0)::test_exprs.composite;
EXPLAIN (COSTS OFF) SELECT * FROM test_exprs.composite WHERE (a, b)::test_exprs.composite < (21, 0);
DROP TABLE test_exprs.composite CASCADE;


/* We use this rel to check 'pathman_hooks_enabled' */
CREATE TABLE test_exprs.canary(val INT4 NOT NULL);
CREATE TABLE test_exprs.canary_copy (LIKE test_exprs.canary);
SELECT create_hash_partitions('test_exprs.canary', 'val', 5);



/*
 * Test HASH
 */

CREATE TABLE test_exprs.hash_rel (
	id		SERIAL PRIMARY KEY,
	value	INTEGER NOT NULL,
	value2  INTEGER NOT NULL
);
INSERT INTO test_exprs.hash_rel (value, value2)
	SELECT val, val * 2 FROM generate_series(1, 5) val;

SELECT COUNT(*) FROM test_exprs.hash_rel;



\set VERBOSITY default

/* Try using constant expression */
SELECT create_hash_partitions('test_exprs.hash_rel', '1 + 1', 4);

/* Try using system attributes */
SELECT create_hash_partitions('test_exprs.hash_rel', 'xmin', 4);

/* Try using subqueries */
SELECT create_hash_partitions('test_exprs.hash_rel',
							  'value, (select oid from pg_class limit 1)',
							  4);

/* Try using mutable expression */
SELECT create_hash_partitions('test_exprs.hash_rel', 'random()', 4);

/* Try using broken parentheses */
SELECT create_hash_partitions('test_exprs.hash_rel', 'value * value2))', 4);

/* Try using missing columns */
SELECT create_hash_partitions('test_exprs.hash_rel', 'value * value3', 4);

/* Check that 'pathman_hooks_enabled' is true (1 partition in plan) */
EXPLAIN (COSTS OFF) INSERT INTO test_exprs.canary_copy
SELECT * FROM test_exprs.canary WHERE val = 1;

\set VERBOSITY terse


SELECT create_hash_partitions('test_exprs.hash_rel', 'value * value2', 4);
SELECT COUNT(*) FROM ONLY test_exprs.hash_rel;
SELECT COUNT(*) FROM test_exprs.hash_rel;

INSERT INTO test_exprs.hash_rel (value, value2)
	SELECT val, val * 2 FROM generate_series(6, 10) val;
SELECT COUNT(*) FROM ONLY test_exprs.hash_rel;
SELECT COUNT(*) FROM test_exprs.hash_rel;

EXPLAIN (COSTS OFF) SELECT * FROM test_exprs.hash_rel WHERE value = 5;
EXPLAIN (COSTS OFF) SELECT * FROM test_exprs.hash_rel WHERE (value * value2) = 5;



/*
 * Test RANGE
 */

CREATE TABLE test_exprs.range_rel (id SERIAL PRIMARY KEY, dt TIMESTAMP NOT NULL, txt TEXT);

INSERT INTO test_exprs.range_rel (dt, txt)
SELECT g, md5(g::TEXT) FROM generate_series('2015-01-01', '2020-04-30', '1 month'::interval) as g;



\set VERBOSITY default

/* Try using constant expression */
SELECT create_range_partitions('test_exprs.range_rel', '''16 years''::interval',
							   '15 years'::INTERVAL, '1 year'::INTERVAL, 10);

/* Try using mutable expression */
SELECT create_range_partitions('test_exprs.range_rel', 'RANDOM()',
							   '15 years'::INTERVAL, '1 year'::INTERVAL, 10);

/* Check that 'pathman_hooks_enabled' is true (1 partition in plan) */
EXPLAIN (COSTS OFF) INSERT INTO test_exprs.canary_copy
SELECT * FROM test_exprs.canary WHERE val = 1;

\set VERBOSITY terse


SELECT create_range_partitions('test_exprs.range_rel', 'AGE(dt, ''2000-01-01''::DATE)',
							   '15 years'::INTERVAL, '1 year'::INTERVAL, 10);
INSERT INTO test_exprs.range_rel_1 (dt, txt) VALUES ('2020-01-01'::DATE, md5('asdf'));
SELECT COUNT(*) FROM test_exprs.range_rel_6;
INSERT INTO test_exprs.range_rel_6 (dt, txt) VALUES ('2020-01-01'::DATE, md5('asdf'));
SELECT COUNT(*) FROM test_exprs.range_rel_6;
EXPLAIN (COSTS OFF) SELECT * FROM test_exprs.range_rel WHERE (AGE(dt, '2000-01-01'::DATE)) = '18 years'::interval;

DROP SCHEMA test_exprs CASCADE;
DROP EXTENSION pg_pathman;
