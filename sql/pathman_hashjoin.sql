/*
 * pathman_hashjoin_1.out and pathman_hashjoin_2.out seem to deal with pgpro's
 * different behaviour. 8edd0e794 (>= 12) Append nodes with single subplan
 * are eliminated, hence pathman_hashjoin_3.out
 *
 * Since 55a1954da16 and 6ef77cf46e8 (>= 13) output of EXPLAIN was changed,
 * now it includes aliases for inherited tables.
 */

\set VERBOSITY terse

SET search_path = 'public';
CREATE SCHEMA pathman;
CREATE EXTENSION pg_pathman SCHEMA pathman;
CREATE SCHEMA test;

CREATE TABLE test.range_rel (
	id	SERIAL PRIMARY KEY,
	dt	TIMESTAMP NOT NULL,
	txt	TEXT);
CREATE INDEX ON test.range_rel (dt);
INSERT INTO test.range_rel (dt, txt)
	SELECT g, md5(g::TEXT) FROM generate_series('2015-01-01', '2015-04-30', '1 day'::interval) as g;
SELECT pathman.create_range_partitions('test.range_rel', 'DT', '2015-01-01'::DATE, '1 month'::INTERVAL);

CREATE TABLE test.num_range_rel (
	id	SERIAL PRIMARY KEY,
	txt	TEXT);
SELECT pathman.create_range_partitions('test.num_range_rel', 'id', 0, 1000, 4);
INSERT INTO test.num_range_rel
	SELECT g, md5(g::TEXT) FROM generate_series(1, 3000) as g;

SET pg_pathman.enable_runtimeappend = OFF;
SET pg_pathman.enable_runtimemergeappend = OFF;
VACUUM;

/*
 * Hash join
 */
SET enable_indexscan = ON;
SET enable_seqscan = OFF;
SET enable_nestloop = OFF;
SET enable_hashjoin = ON;
SET enable_mergejoin = OFF;

EXPLAIN (COSTS OFF)
SELECT * FROM test.range_rel j1
JOIN test.range_rel j2 on j2.id = j1.id
JOIN test.num_range_rel j3 on j3.id = j1.id
WHERE j1.dt < '2015-03-01' AND j2.dt >= '2015-02-01' ORDER BY j2.dt;

DROP SCHEMA test CASCADE;
DROP EXTENSION pg_pathman CASCADE;
DROP SCHEMA pathman CASCADE;
