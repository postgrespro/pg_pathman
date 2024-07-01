\set VERBOSITY terse

SET search_path = 'public';
CREATE SCHEMA pathman;
CREATE EXTENSION pg_pathman SCHEMA pathman;
CREATE SCHEMA test;

CREATE TABLE test.range_rel (
	id	SERIAL PRIMARY KEY,
	dt	DATE NOT NULL
);
CREATE TABLE test.r2 (LIKE test.range_rel);
ALTER TABLE test.range_rel ATTACH PARTITION test.r2
	FOR VALUES FROM ('2015-05-01') TO ('2015-06-01');

INSERT INTO test.range_rel (dt)
SELECT g FROM generate_series('2015-01-01', '2015-04-30', '1 day'::interval) AS g;
SELECT pathman.create_range_partitions('test.range_rel', 'dt',
	'2015-01-01'::DATE, '1 month'::INTERVAL);

SELECT * FROM pathman.pathman_partition_list;
ALTER TABLE test.range_rel ATTACH PARTITION test.r2
	FOR VALUES IN ('2015-05-01', '2015-06-01');
ALTER TABLE test.range_rel ATTACH PARTITION test.r2
	FOR VALUES FROM ('2014-05-01') TO ('2015-06-01');
ALTER TABLE test.range_rel ATTACH PARTITION test.r2
	FOR VALUES FROM ('2015-05-01') TO ('2015-06-01');
SELECT * FROM pathman.pathman_partition_list;
\d+ test.r2;
ALTER TABLE test.range_rel DETACH PARTITION test.r2;
SELECT * FROM pathman.pathman_partition_list;
\d+ test.r2;

CREATE TABLE test.r4 PARTITION OF test.range_rel
	FOR VALUES IN ('2015-05-01', '2015-06-01');
CREATE TABLE test.r4 PARTITION OF test.range_rel
	FOR VALUES FROM ('2014-05-01') TO ('2015-06-01');
CREATE TABLE test.r4 PARTITION OF test.range_rel
	FOR VALUES FROM ('2015-06-01') TO ('2016-01-01');
\d+ test.r4;

/* Note: PG-10 doesn't support ATTACH PARTITION ... DEFAULT */
ALTER TABLE IF EXISTS test.nonexistent_table ATTACH PARTITION baz FOR VALUES IN (42);
ALTER TABLE IF EXISTS test.nonexistent_table DETACH PARTITION baz;

DROP TABLE test.r2 CASCADE;
DROP TABLE test.range_rel CASCADE;
DROP SCHEMA test;
DROP EXTENSION pg_pathman CASCADE;
DROP SCHEMA pathman;
