\set VERBOSITY terse

SET search_path = 'public';
CREATE EXTENSION pg_pathman;
CREATE SCHEMA test_bgw;


/*
 * Tests for SpawnPartitionsWorker
 */

/* int4, size of Datum == 4 */
CREATE TABLE test_bgw.test_1(val INT4 NOT NULL);
SELECT create_range_partitions('test_bgw.test_1', 'val', 1, 5, 2);

SELECT set_spawn_using_bgw('test_bgw.test_1', true);
INSERT INTO test_bgw.test_1 VALUES (11);
SELECT * FROM pathman_partition_list ORDER BY partition; /* should contain 3 partitions */

DROP TABLE test_bgw.test_1 CASCADE;


/* int8, size of Datum == 8 */
CREATE TABLE test_bgw.test_2(val INT8 NOT NULL);
SELECT create_range_partitions('test_bgw.test_2', 'val', 1, 5, 2);

SELECT set_spawn_using_bgw('test_bgw.test_2', true);
INSERT INTO test_bgw.test_2 VALUES (11);
SELECT * FROM pathman_partition_list ORDER BY partition; /* should contain 3 partitions */

DROP TABLE test_bgw.test_2 CASCADE;


/* numeric, size of Datum == var */
CREATE TABLE test_bgw.test_3(val NUMERIC NOT NULL);
SELECT create_range_partitions('test_bgw.test_3', 'val', 1, 5, 2);

SELECT set_spawn_using_bgw('test_bgw.test_3', true);
INSERT INTO test_bgw.test_3 VALUES (11);
SELECT * FROM pathman_partition_list ORDER BY partition; /* should contain 3 partitions */

DROP TABLE test_bgw.test_3 CASCADE;


/* date, size of Datum == var */
CREATE TABLE test_bgw.test_4(val DATE NOT NULL);
SELECT create_range_partitions('test_bgw.test_4', 'val', '20170213'::date, '1 day'::interval, 2);

SELECT set_spawn_using_bgw('test_bgw.test_4', true);
INSERT INTO test_bgw.test_4 VALUES ('20170215');
SELECT * FROM pathman_partition_list ORDER BY partition; /* should contain 3 partitions */

DROP TABLE test_bgw.test_4 CASCADE;



DROP SCHEMA test_bgw CASCADE;
DROP EXTENSION pg_pathman;
