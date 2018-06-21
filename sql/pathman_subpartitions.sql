\set VERBOSITY terse

CREATE EXTENSION pg_pathman;
CREATE SCHEMA subpartitions;

:gdb
select pg_sleep(5);



/* Create two level partitioning structure */
CREATE TABLE subpartitions.abc(a INTEGER NOT NULL, b INTEGER NOT NULL);
INSERT INTO subpartitions.abc SELECT i, i FROM generate_series(1, 200, 20) as i;
SELECT create_range_partitions('subpartitions.abc', 'a', 0, 100, 2);
SELECT create_hash_partitions('subpartitions.abc_1', 'a', 3);
SELECT create_hash_partitions('subpartitions.abc_2', 'b', 2);
SELECT * FROM pathman_partition_list;
SELECT tableoid::regclass, * FROM subpartitions.abc ORDER BY a, b;

/* Insert should result in creation of new subpartition */
SELECT append_range_partition('subpartitions.abc', 'subpartitions.abc_3');
SELECT create_range_partitions('subpartitions.abc_3', 'b', 200, 10, 2);
SELECT * FROM pathman_partition_list WHERE parent = 'subpartitions.abc_3'::regclass;
INSERT INTO subpartitions.abc VALUES (215, 215);
SELECT * FROM pathman_partition_list WHERE parent = 'subpartitions.abc_3'::regclass;
SELECT tableoid::regclass, * FROM subpartitions.abc WHERE a = 215 AND b = 215 ORDER BY a, b;

/* Pruning tests */
EXPLAIN (COSTS OFF) SELECT * FROM subpartitions.abc WHERE a  < 150;
EXPLAIN (COSTS OFF) SELECT * FROM subpartitions.abc WHERE b  = 215;
EXPLAIN (COSTS OFF) SELECT * FROM subpartitions.abc WHERE a  = 215 AND b  = 215;
EXPLAIN (COSTS OFF) SELECT * FROM subpartitions.abc WHERE a >= 210 AND b >= 210;

CREATE OR REPLACE FUNCTION check_multilevel_queries()
RETURNS VOID AS
$$
BEGIN
	IF NOT EXISTS(SELECT * FROM (SELECT tableoid::regclass, *
				  FROM subpartitions.abc
				  WHERE a = 215 AND b = 215
			      ORDER BY a, b) t1)
	THEN
		RAISE EXCEPTION 'should be at least one record in result';
	END IF;
END
$$ LANGUAGE plpgsql;
SELECT check_multilevel_queries();
DROP FUNCTION check_multilevel_queries();

/* Multilevel partitioning with updates */
CREATE OR REPLACE FUNCTION subpartitions.partitions_tree(
	rel REGCLASS,
	level TEXT DEFAULT ' '
)
RETURNS SETOF TEXT AS
$$
DECLARE
	partition REGCLASS;
	subpartition TEXT;
BEGIN
	IF rel IS NULL THEN
		RETURN;
	END IF;

	RETURN NEXT rel::TEXT;

	FOR partition IN (SELECT l.partition FROM pathman_partition_list l WHERE parent = rel)
	LOOP
		FOR subpartition IN (SELECT subpartitions.partitions_tree(partition, level || ' '))
		LOOP
			RETURN NEXT level || subpartition::TEXT;
		END LOOP;
	END LOOP;
END
$$ LANGUAGE plpgsql;

SELECT append_range_partition('subpartitions.abc', 'subpartitions.abc_4');
SELECT create_hash_partitions('subpartitions.abc_4', 'b', 2);
SELECT subpartitions.partitions_tree('subpartitions.abc');
DROP TABLE subpartitions.abc CASCADE;


/* Test that update works correctly */
SET pg_pathman.enable_partitionrouter = ON;

CREATE TABLE subpartitions.abc(a INTEGER NOT NULL, b INTEGER NOT NULL);
SELECT create_range_partitions('subpartitions.abc', 'a', 0, 100, 2);
SELECT create_range_partitions('subpartitions.abc_1', 'b', 0, 50, 2); /* 0 - 100 */
SELECT create_range_partitions('subpartitions.abc_2', 'b', 0, 50, 2); /* 100 - 200 */

INSERT INTO subpartitions.abc SELECT 25, 25 FROM generate_series(1, 10);
SELECT tableoid::regclass, * FROM subpartitions.abc;	/* Should be in subpartitions.abc_1_1 */

UPDATE subpartitions.abc SET a = 125 WHERE a = 25 and b = 25;
SELECT tableoid::regclass, * FROM subpartitions.abc;	/* Should be in subpartitions.abc_2_1 */

UPDATE subpartitions.abc SET b = 75  WHERE a = 125 and b = 25;
SELECT tableoid::regclass, * FROM subpartitions.abc;	/* Should be in subpartitions.abc_2_2 */

UPDATE subpartitions.abc SET b = 125 WHERE a = 125 and b = 75;
SELECT tableoid::regclass, * FROM subpartitions.abc;	/* Should create subpartitions.abc_2_3 */

/* split_range_partition */
SELECT split_range_partition('subpartitions.abc_2', 150);
SELECT split_range_partition('subpartitions.abc_2_2', 75);
SELECT subpartitions.partitions_tree('subpartitions.abc');

/* merge_range_partitions */
SELECT append_range_partition('subpartitions.abc', 'subpartitions.abc_3'); /* 200 - 300 */
select merge_range_partitions('subpartitions.abc_2', 'subpartitions.abc_3');
select merge_range_partitions('subpartitions.abc_2_1', 'subpartitions.abc_2_2');

DROP TABLE subpartitions.abc CASCADE;

/* subpartitions on same expressions */
CREATE TABLE subpartitions.abc(a INTEGER NOT NULL);
INSERT INTO subpartitions.abc SELECT i FROM generate_series(1, 200, 20) as i;
SELECT create_range_partitions('subpartitions.abc', 'a', 0, 100, 4);
SELECT create_range_partitions('subpartitions.abc_1', 'a', 0, 11, 9); /* not multiple */
SELECT create_range_partitions('subpartitions.abc_2', 'a', 150, 11, 8); /* start_value should be lower */
SELECT create_range_partitions('subpartitions.abc_3', 'a', 200, 11, 20); /* too big p_count */
SELECT create_range_partitions('subpartitions.abc_4', 'a', ARRAY[301, 350, 400]); /* bounds check */
SELECT create_range_partitions('subpartitions.abc_4', 'a', ARRAY[300, 450, 500]); /* bounds check */
SELECT create_range_partitions('subpartitions.abc_4', 'a', ARRAY[300, 350, 450]); /* bounds check */
SELECT * FROM pathman_partition_list;
SELECT append_range_partition('subpartitions.abc_1'::regclass);
SELECT append_range_partition('subpartitions.abc_1'::regclass);
DROP TABLE subpartitions.abc_1_10;

/* detach_range_partition */
SELECt detach_range_partition('subpartitions.abc_1');

/* attach_range_partition */
CREATE TABLE subpartitions.abc_c(LIKE subpartitions.abc_1 INCLUDING ALL);
SELECT attach_range_partition('subpartitions.abc_1', 'subpartitions.abc_c', 98, 110); /* fail */
SELECT attach_range_partition('subpartitions.abc_1', 'subpartitions.abc_c', 100, 110); /* fail */
SELECT attach_range_partition('subpartitions.abc_1', 'subpartitions.abc_c', 99, 110); /* ok */

DROP TABLE subpartitions.abc CASCADE;

/* subpartitions on same expression but dates */
CREATE TABLE subpartitions.abc(a DATE NOT NULL);
INSERT INTO subpartitions.abc SELECT '2017-10-02'::DATE + i  FROM generate_series(1, 200, 20) as i;
SELECT create_range_partitions('subpartitions.abc', 'a', '2017-10-02'::DATE, '1 month'::INTERVAL);
SELECT create_range_partitions('subpartitions.abc_1', 'a', '2017-10-02'::DATE + 1,
	'32 day'::INTERVAL, 10); /* not multiple, and limited p_count */

DROP TABLE subpartitions.abc CASCADE;
DROP SCHEMA subpartitions CASCADE;
DROP EXTENSION pg_pathman;
