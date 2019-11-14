/*
 * Since 8edd0e794 (>= 12) Append nodes with single subplan are eliminated,
 * causing different output; pathman_subpartitions_1.out is the updated version.
 */

\set VERBOSITY terse

CREATE EXTENSION pg_pathman;
CREATE SCHEMA subpartitions;



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
SELECT tableoid::regclass, * FROM subpartitions.abc;	/* subpartitions.abc_1_1 */

UPDATE subpartitions.abc SET a = 125 WHERE a = 25 and b = 25;
SELECT tableoid::regclass, * FROM subpartitions.abc;	/* subpartitions.abc_2_1 */

UPDATE subpartitions.abc SET b = 75  WHERE a = 125 and b = 25;
SELECT tableoid::regclass, * FROM subpartitions.abc;	/* subpartitions.abc_2_2 */

UPDATE subpartitions.abc SET b = 125 WHERE a = 125 and b = 75;
SELECT tableoid::regclass, * FROM subpartitions.abc;	/* subpartitions.abc_2_3 */


/* split_range_partition */
SELECT split_range_partition('subpartitions.abc_2', 150);	/* FAIL */
SELECT split_range_partition('subpartitions.abc_2_2', 75);	/* OK */
SELECT subpartitions.partitions_tree('subpartitions.abc');


/* merge_range_partitions */
TRUNCATE subpartitions.abc;
INSERT INTO subpartitions.abc VALUES (150, 0);

SELECT append_range_partition('subpartitions.abc', 'subpartitions.abc_3');			/* 200 - 300 */
INSERT INTO subpartitions.abc VALUES (250, 50);

SELECT merge_range_partitions('subpartitions.abc_2', 'subpartitions.abc_3');		/* OK */
SELECT tableoid::regclass, * FROM subpartitions.abc ORDER BY a, b;

SELECT merge_range_partitions('subpartitions.abc_2_1', 'subpartitions.abc_2_2');	/* OK */
SELECT tableoid::regclass, * FROM subpartitions.abc ORDER BY a, b;


DROP TABLE subpartitions.abc CASCADE;


/* Check insert & update with dropped columns */
CREATE TABLE subpartitions.abc(a int, b int, c int, id1 int not null, id2 int not null, val serial);
SELECT create_range_partitions('subpartitions.abc', 'id1', 0, 100, 2);
ALTER TABLE subpartitions.abc DROP COLUMN c;
SELECT prepend_range_partition('subpartitions.abc');
ALTER TABLE subpartitions.abc DROP COLUMN b;
SELECT create_range_partitions('subpartitions.abc_3', 'id2', 0, 10, 3);
ALTER TABLE subpartitions.abc DROP COLUMN a;
SELECT prepend_range_partition('subpartitions.abc_3');

SELECT * FROM pathman_partition_list ORDER BY parent, partition;

INSERT INTO subpartitions.abc VALUES (10, 0), (110, 0), (-1, 0), (-1, -1);
SELECT tableoid::regclass, * FROM subpartitions.abc ORDER BY id1, id2, val;

SET pg_pathman.enable_partitionrouter = ON;
UPDATE subpartitions.abc SET id1 = -1, id2 = -1 RETURNING tableoid::regclass, *;

DROP TABLE subpartitions.abc CASCADE;


--- basic check how rowmark plays along with subparts; PGPRO-2755
CREATE TABLE subpartitions.a1(n1 integer);
CREATE TABLE subpartitions.a2(n1 integer not null, n2 integer not null);
SELECT create_range_partitions('subpartitions.a2', 'n1', 1, 2, 0);

SELECT add_range_partition('subpartitions.a2', 10, 20, 'subpartitions.a2_1020');
SELECT create_range_partitions('subpartitions.a2_1020'::regclass, 'n2'::text, array[30,40], array['subpartitions.a2_1020_3040']);
INSERT INTO subpartitions.a2 VALUES (10, 30), (11, 31), (12, 32), (19, 39);
INSERT INTO subpartitions.a1 VALUES (12), (19), (20);

SELECT a2.* FROM subpartitions.a1 JOIN subpartitions.a2 ON a2.n1=a1.n1 FOR UPDATE;

DROP TABLE subpartitions.a2 CASCADE;
DROP TABLE subpartitions.a1;


DROP SCHEMA subpartitions CASCADE;
DROP EXTENSION pg_pathman;
