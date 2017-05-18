\set VERBOSITY terse

CREATE EXTENSION pg_pathman;

/* Create two level partitioning structure */
CREATE TABLE abc(a INTEGER NOT NULL, b INTEGER NOT NULL);
INSERT INTO abc SELECT i, i FROM generate_series(1, 200, 20) as i;
SELECT create_range_partitions('abc', 'a', 0, 100, 2);
SELECT create_hash_partitions('abc_1', 'a', 3);
SELECT create_hash_partitions('abc_2', 'b', 2);
SELECT * FROM pathman_partition_list;
SELECT tableoid::regclass, * FROM abc;

/* Insert should result in creating of new subpartition */
SELECT append_range_partition('abc', 'abc_3');
SELECT create_range_partitions('abc_3', 'b', 200, 10, 2);
SELECT * FROM pathman_partition_list WHERE parent = 'abc_3'::regclass;
INSERT INTO abc VALUES (215, 215);
SELECT * FROM pathman_partition_list WHERE parent = 'abc_3'::regclass;
SELECT tableoid::regclass, * FROM abc WHERE a = 215 AND b = 215;

/* Pruning tests */
EXPLAIN (COSTS OFF) SELECT * FROM abc WHERE a < 150;
EXPLAIN (COSTS OFF) SELECT * FROM abc WHERE b = 215;
EXPLAIN (COSTS OFF) SELECT * FROM abc WHERE a = 215 AND b = 215;
EXPLAIN (COSTS OFF) SELECT * FROM abc WHERE a >= 210 and b >= 210;

/* Multilevel partitioning with update triggers */
CREATE OR REPLACE FUNCTION partitions_tree(rel REGCLASS)
RETURNS SETOF REGCLASS AS
$$
DECLARE
	partition		REGCLASS;
	subpartition	REGCLASS;
BEGIN
	IF rel IS NULL THEN
		RETURN;
	END IF;

	RETURN NEXT rel;

	FOR partition IN (SELECT l.partition FROM pathman_partition_list l WHERE parent = rel)
	LOOP
		FOR subpartition IN (SELECT partitions_tree(partition))
		LOOP
			RETURN NEXT subpartition;
		END LOOP;
	END LOOP;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_triggers(rel REGCLASS)
RETURNS SETOF TEXT AS
$$
DECLARE
	def TEXT;
BEGIN
	FOR def IN (SELECT pg_get_triggerdef(oid) FROM pg_trigger WHERE tgrelid = rel)
	LOOP
		RETURN NEXT def;
	END LOOP;

	RETURN;
END;
$$ LANGUAGE plpgsql;

SELECT create_update_triggers('abc_1');	/* Cannot perform on partition */
SELECT create_update_triggers('abc');	/* Only on parent */
SELECT p, get_triggers(p) FROM partitions_tree('abc') as p;

SELECT append_range_partition('abc', 'abc_4');
SELECT create_hash_partitions('abc_4', 'b', 2); /* Triggers should automatically
												 * be created on subpartitions */
SELECT p, get_triggers(p) FROM partitions_tree('abc_4') as p;
SELECT drop_triggers('abc_1');			/* Cannot perform on partition */
SELECT drop_triggers('abc');			/* Only on parent */
SELECT p, get_triggers(p) FROM partitions_tree('abc') as p;	/* No partitions */

DROP TABLE abc CASCADE;

/* Test that update trigger words correclty */
CREATE TABLE abc(a INTEGER NOT NULL, b INTEGER NOT NULL);
SELECT create_range_partitions('abc', 'a', 0, 100, 2);
SELECT create_range_partitions('abc_1', 'b', 0, 50, 2);
SELECT create_range_partitions('abc_2', 'b', 0, 50, 2);
SELECT create_update_triggers('abc');

INSERT INTO abc VALUES (25, 25);		/* Should get into abc_1_1 */
SELECT tableoid::regclass, * FROM abc;
UPDATE abc SET a = 125 WHERE a = 25 and b = 25;
SELECT tableoid::regclass, * FROM abc;	/* Should be in abc_2_1 */
UPDATE abc SET b = 75 WHERE a = 125 and b = 25;
SELECT tableoid::regclass, * FROM abc;	/* Should be in abc_2_2 */
UPDATE abc SET b = 125 WHERE a = 125 and b = 75;
SELECT tableoid::regclass, * FROM abc;	/* Should create partition abc_2_3 */

DROP TABLE abc CASCADE;

DROP EXTENSION pg_pathman;