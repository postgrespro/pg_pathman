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


/* test error handling in BGW */
CREATE TABLE test_bgw.test_5(val INT4 NOT NULL);
SELECT create_range_partitions('test_bgw.test_5', 'val', 1, 10, 2);

CREATE OR REPLACE FUNCTION test_bgw.abort_xact(args JSONB)
RETURNS VOID AS $$
BEGIN
	RAISE EXCEPTION 'aborting xact!';
END
$$ language plpgsql;

SELECT set_spawn_using_bgw('test_bgw.test_5', true);
SELECT set_init_callback('test_bgw.test_5', 'test_bgw.abort_xact(jsonb)');
INSERT INTO test_bgw.test_5 VALUES (-100);
SELECT * FROM pathman_partition_list ORDER BY partition; /* should contain 3 partitions */

DROP FUNCTION test_bgw.abort_xact(args JSONB);
DROP TABLE test_bgw.test_5 CASCADE;



/*
 * Tests for ConcurrentPartWorker
 */

CREATE TABLE test_bgw.conc_part(id INT4 NOT NULL);
INSERT INTO test_bgw.conc_part SELECT generate_series(1, 500);
SELECT create_hash_partitions('test_bgw.conc_part', 'id', 5, false);

BEGIN;
/* Also test FOR SHARE/UPDATE conflicts in BGW */
SELECT * FROM test_bgw.conc_part ORDER BY id LIMIT 1 FOR SHARE;
/* Run partitioning bgworker */
SELECT partition_table_concurrently('test_bgw.conc_part', 10, 1);
/* Wait until bgworker starts */
SELECT pg_sleep(1);
ROLLBACK;

/* Wait until it finises */
DO $$
DECLARE
	ops			int8;
	rows		int8;
	rows_old	int8 := 0;
	i			int4 := 0; -- protect from endless loop
BEGIN
	LOOP
		-- get total number of processed rows
		SELECT processed
		FROM pathman_concurrent_part_tasks
		WHERE relid = 'test_bgw.conc_part'::regclass
		INTO rows;

		-- get number of partitioning tasks
		GET DIAGNOSTICS ops = ROW_COUNT;

		IF ops > 0 THEN
			PERFORM pg_sleep(0.2);

			ASSERT rows IS NOT NULL;

			IF rows_old = rows THEN
				i = i + 1;
			ELSIF rows < rows_old THEN
				RAISE EXCEPTION 'rows is decreasing: new %, old %', rows, rows_old;
			ELSIF rows > 500 THEN
				RAISE EXCEPTION 'processed % rows', rows;
			END IF;
		ELSE
			EXIT; -- exit loop
		END IF;

		IF i > 500 THEN
			RAISE WARNING 'looks like partitioning bgw is stuck!';
			EXIT; -- exit loop
		END IF;

		rows_old = rows;
	END LOOP;
END
$$ LANGUAGE plpgsql;

/* Check amount of tasks and rows in parent and partitions */
SELECT count(*) FROM pathman_concurrent_part_tasks;
SELECT count(*) FROM ONLY test_bgw.conc_part;
SELECT count(*) FROM test_bgw.conc_part;

DROP TABLE test_bgw.conc_part CASCADE;



DROP SCHEMA test_bgw;
DROP EXTENSION pg_pathman;
