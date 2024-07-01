/*
 * Test simple CTE queries.
 * Since 12 (608b167f9f), CTEs which are scanned once are no longer an
 * optimization fence, which changes practically all plans here. There is
 * an option to forcibly make them MATERIALIZED, but we also need to run tests
 * on older versions, so create pathman_cte_1.out instead.
 */

\set VERBOSITY terse

SET search_path = 'public';
CREATE EXTENSION pg_pathman;
CREATE SCHEMA test_cte;

CREATE TABLE test_cte.range_rel (
	id	INT4,
	dt	TIMESTAMP NOT NULL,
	txt	TEXT);

INSERT INTO test_cte.range_rel (dt, txt)
SELECT g, md5(g::TEXT)
FROM generate_series('2015-01-01', '2015-04-30', '1 day'::interval) AS g;

SELECT create_range_partitions('test_cte.range_rel', 'DT', '2015-01-01'::DATE, '1 month'::INTERVAL);

/* perform a query */
EXPLAIN (COSTS OFF)
	WITH ttt AS (SELECT * FROM test_cte.range_rel WHERE dt >= '2015-02-01' AND dt < '2015-03-15')
SELECT * FROM ttt;

DROP TABLE test_cte.range_rel CASCADE;


CREATE TABLE test_cte.hash_rel (
	id		INT4,
	value	INTEGER NOT NULL);
INSERT INTO test_cte.hash_rel VALUES (1, 1);
INSERT INTO test_cte.hash_rel VALUES (2, 2);
INSERT INTO test_cte.hash_rel VALUES (3, 3);
SELECT create_hash_partitions('test_cte.hash_rel', 'value', 3);

/* perform a query */
EXPLAIN (COSTS OFF)
	WITH ttt AS (SELECT * FROM test_cte.hash_rel WHERE value = 2)
SELECT * FROM ttt;

DROP TABLE test_cte.hash_rel CASCADE;



/*
 * Test CTE query - by @parihaaraka (add varno to WalkerContext)
 */
CREATE TABLE test_cte.cte_del_xacts (id BIGSERIAL PRIMARY KEY, pdate DATE NOT NULL);

INSERT INTO test_cte.cte_del_xacts (pdate)
SELECT gen_date
FROM generate_series('2016-01-01'::date, '2016-04-9'::date, '1 day') AS gen_date;

CREATE TABLE test_cte.cte_del_xacts_specdata
(
	tid BIGINT PRIMARY KEY,
	test_mode SMALLINT,
	state_code SMALLINT NOT NULL DEFAULT 8,
	regtime TIMESTAMP WITHOUT TIME ZONE NOT NULL
);

INSERT INTO test_cte.cte_del_xacts_specdata VALUES (1, 1, 1, current_timestamp); /* for subquery test */

/* create 2 partitions */
SELECT create_range_partitions('test_cte.cte_del_xacts'::regclass, 'pdate',
							   '2016-01-01'::date, '50 days'::interval);

EXPLAIN (COSTS OFF)
WITH tmp AS (
	SELECT tid, test_mode, regtime::DATE AS pdate, state_code
	FROM test_cte.cte_del_xacts_specdata)
DELETE FROM test_cte.cte_del_xacts t USING tmp
WHERE t.id = tmp.tid AND t.pdate = tmp.pdate AND tmp.test_mode > 0;

SELECT drop_partitions('test_cte.cte_del_xacts'); /* now drop partitions */

/* create 1 partition */
SELECT create_range_partitions('test_cte.cte_del_xacts'::regclass, 'pdate',
							   '2016-01-01'::date, '1 year'::interval);

/* parent enabled! */
SELECT set_enable_parent('test_cte.cte_del_xacts', true);
EXPLAIN (COSTS OFF)
WITH tmp AS (
	SELECT tid, test_mode, regtime::DATE AS pdate, state_code
	FROM test_cte.cte_del_xacts_specdata)
DELETE FROM test_cte.cte_del_xacts t USING tmp
WHERE t.id = tmp.tid AND t.pdate = tmp.pdate AND tmp.test_mode > 0;

/* parent disabled! */
SELECT set_enable_parent('test_cte.cte_del_xacts', false);
EXPLAIN (COSTS OFF)
WITH tmp AS (
	SELECT tid, test_mode, regtime::DATE AS pdate, state_code
	FROM test_cte.cte_del_xacts_specdata)
DELETE FROM test_cte.cte_del_xacts t USING tmp
WHERE t.id = tmp.tid AND t.pdate = tmp.pdate AND tmp.test_mode > 0;

/* create stub pl/PgSQL function */
CREATE OR REPLACE FUNCTION test_cte.cte_del_xacts_stab(name TEXT)
RETURNS smallint AS
$$
begin
	return 2::smallint;
end
$$
LANGUAGE plpgsql STABLE;

/* test subquery planning */
WITH tmp AS (
	SELECT tid FROM test_cte.cte_del_xacts_specdata
	WHERE state_code != test_cte.cte_del_xacts_stab('test'))
SELECT * FROM test_cte.cte_del_xacts t JOIN tmp ON t.id = tmp.tid;

/* test subquery planning (one more time) */
WITH tmp AS (
	SELECT tid FROM test_cte.cte_del_xacts_specdata
	WHERE state_code != test_cte.cte_del_xacts_stab('test'))
SELECT * FROM test_cte.cte_del_xacts t JOIN tmp ON t.id = tmp.tid;

DROP FUNCTION test_cte.cte_del_xacts_stab(TEXT);
DROP TABLE test_cte.cte_del_xacts, test_cte.cte_del_xacts_specdata CASCADE;


/* Test recursive CTE */
CREATE TABLE test_cte.recursive_cte_test_tbl(id INT NOT NULL, name TEXT NOT NULL);
SELECT create_hash_partitions('test_cte.recursive_cte_test_tbl', 'id', 2);

INSERT INTO test_cte.recursive_cte_test_tbl (id, name)
SELECT id, 'name'||id FROM generate_series(1,100) f(id);

INSERT INTO test_cte.recursive_cte_test_tbl (id, name)
SELECT id, 'name'||(id + 1) FROM generate_series(1,100) f(id);

INSERT INTO test_cte.recursive_cte_test_tbl (id, name)
SELECT id, 'name'||(id + 2) FROM generate_series(1,100) f(id);

SELECT * FROM test_cte.recursive_cte_test_tbl WHERE id = 5;

WITH RECURSIVE test AS (
	SELECT min(name) AS name
	FROM test_cte.recursive_cte_test_tbl
	WHERE id = 5
	UNION ALL
	SELECT (SELECT min(name)
			FROM test_cte.recursive_cte_test_tbl
			WHERE id = 5 AND name > test.name)
	FROM test
	WHERE name IS NOT NULL)
SELECT * FROM test;



DROP TABLE test_cte.recursive_cte_test_tbl CASCADE;
DROP SCHEMA test_cte;
DROP EXTENSION pg_pathman;
