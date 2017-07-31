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

\q
/* Test that update works correctly */
SET pg_pathman.enable_partitionupdate=on;
CREATE TABLE subpartitions.abc(a INTEGER NOT NULL, b INTEGER NOT NULL);
SELECT create_range_partitions('subpartitions.abc', 'a', 0, 100, 2);
SELECT create_range_partitions('subpartitions.abc_1', 'b', 0, 50, 2);
SELECT create_range_partitions('subpartitions.abc_2', 'b', 0, 50, 2);

INSERT INTO subpartitions.abc VALUES (25, 25);
SELECT tableoid::regclass, * FROM subpartitions.abc;	/* Should be in subpartitions.abc_1_1 */

UPDATE subpartitions.abc SET a = 125 WHERE a = 25 and b = 25;
SELECT tableoid::regclass, * FROM subpartitions.abc;	/* Should be in subpartitions.abc_2_1 */

UPDATE subpartitions.abc SET b = 75  WHERE a = 125 and b = 25;
SELECT tableoid::regclass, * FROM subpartitions.abc;	/* Should be in subpartitions.abc_2_2 */

UPDATE subpartitions.abc SET b = 125 WHERE a = 125 and b = 75;
SELECT tableoid::regclass, * FROM subpartitions.abc;	/* Should create subpartitions.abc_2_3 */

DROP TABLE subpartitions.abc CASCADE;
DROP SCHEMA subpartitions CASCADE;
DROP EXTENSION pg_pathman;
