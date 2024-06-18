/*
 * ---------------------------------------------
 *  NOTE: This test behaves differenly on PgPro
 * ---------------------------------------------
 *
 * --------------------
 *  pathman_only_1.sql
 * --------------------
 * Since 608b167f9f in PostgreSQL 12, CTEs which are scanned once are no longer
 * an optimization fence, which changes practically all plans here. There is
 * an option to forcibly make them MATERIALIZED, but we also need to run tests
 * on older versions, so create pathman_only_1.out instead.
 *
 * --------------------
 *  pathman_only_2.sql
 * --------------------
 * Since 55a1954da16 and 6ef77cf46e8 in PostgreSQL 13, output of EXPLAIN was
 * changed, now it includes aliases for inherited tables.
 *
 * --------------------
 *  pathman_only_3.sql
 * --------------------
 * Since a5fc46414de in PostgreSQL 16, the order of the operands was changed,
 * which affected the output of the "Prune by" in EXPLAIN.
 *
 * --------------------
 *  pathman_only_4.sql
 * --------------------
 * Since fd0398fcb09 in PostgreSQL 17, output of EXPLAIN was
 * changed, now it displays SubPlan nodes and output parameters.
 */

\set VERBOSITY terse

SET search_path = 'public';
CREATE EXTENSION pg_pathman;
CREATE SCHEMA test_only;



/* Test special case: ONLY statement with not-ONLY for partitioned table */
CREATE TABLE test_only.from_only_test(val INT NOT NULL);
INSERT INTO test_only.from_only_test SELECT generate_series(1, 20);
SELECT create_range_partitions('test_only.from_only_test', 'val', 1, 2);

VACUUM ANALYZE;

/* should be OK */
EXPLAIN (COSTS OFF)
SELECT * FROM ONLY test_only.from_only_test
UNION SELECT * FROM test_only.from_only_test;

/* should be OK */
EXPLAIN (COSTS OFF)
SELECT * FROM test_only.from_only_test
UNION SELECT * FROM ONLY test_only.from_only_test;

/* should be OK */
EXPLAIN (COSTS OFF)
SELECT * FROM test_only.from_only_test
UNION SELECT * FROM test_only.from_only_test
UNION SELECT * FROM ONLY test_only.from_only_test;

/* should be OK */
EXPLAIN (COSTS OFF)
SELECT * FROM ONLY test_only.from_only_test
UNION SELECT * FROM test_only.from_only_test
UNION SELECT * FROM test_only.from_only_test;

/* not ok, ONLY|non-ONLY in one query (this is not the case for PgPro) */
EXPLAIN (COSTS OFF)
SELECT * FROM test_only.from_only_test a
JOIN ONLY test_only.from_only_test b USING(val);

/* should be OK */
EXPLAIN (COSTS OFF)
WITH q1 AS (SELECT * FROM test_only.from_only_test),
	 q2 AS (SELECT * FROM ONLY test_only.from_only_test)
SELECT * FROM q1 JOIN q2 USING(val);

/* should be OK */
EXPLAIN (COSTS OFF)
WITH q1 AS (SELECT * FROM ONLY test_only.from_only_test)
SELECT * FROM test_only.from_only_test JOIN q1 USING(val);

/* should be OK */
EXPLAIN (COSTS OFF)
SELECT * FROM test_only.from_only_test
WHERE val = (SELECT val FROM ONLY test_only.from_only_test
			 ORDER BY val ASC
			 LIMIT 1);



DROP TABLE test_only.from_only_test CASCADE;
DROP SCHEMA test_only;
DROP EXTENSION pg_pathman;
