\set VERBOSITY terse
SET search_path = 'public';
CREATE EXTENSION pg_pathman;
CREATE SCHEMA callbacks;



/* callback #1 */
CREATE OR REPLACE FUNCTION callbacks.abc_on_part_created_callback(args JSONB)
RETURNS VOID AS $$
BEGIN
	RAISE WARNING 'callback arg: %', args::TEXT;
END
$$ language plpgsql;

/* callback #2 */
CREATE OR REPLACE FUNCTION public.dummy_cb(args JSONB)
RETURNS VOID AS $$
BEGIN
END
$$ language plpgsql;



CREATE TABLE callbacks.abc(a serial, b int);
SELECT create_range_partitions('callbacks.abc', 'a', 1, 100, 2);

SELECT set_init_callback('callbacks.abc', 'public.dummy_cb(jsonb)');

/* check that callback is schema-qualified */
SELECT init_callback FROM pathman_config_params
WHERE partrel = 'callbacks.abc'::REGCLASS;

/* reset callback */
SELECT set_init_callback('callbacks.abc');

/* should return NULL */
SELECT init_callback FROM pathman_config_params
WHERE partrel = 'callbacks.abc'::REGCLASS;

SELECT set_init_callback('callbacks.abc',
						 'callbacks.abc_on_part_created_callback(jsonb)');

/* check that callback is schema-qualified */
SELECT init_callback FROM pathman_config_params
WHERE partrel = 'callbacks.abc'::REGCLASS;

DROP TABLE callbacks.abc CASCADE;


/* set callback to be called on RANGE partitions */
CREATE TABLE callbacks.abc(a serial, b int);
SELECT create_range_partitions('callbacks.abc', 'a', 1, 100, 2);

SELECT set_init_callback('callbacks.abc',
						 'callbacks.abc_on_part_created_callback(jsonb)');

INSERT INTO callbacks.abc VALUES (123, 1);
INSERT INTO callbacks.abc VALUES (223, 1); /* show warning */

SELECT set_spawn_using_bgw('callbacks.abc', true);
SELECT get_number_of_partitions('callbacks.abc');
INSERT INTO callbacks.abc VALUES (323, 1);
SELECT get_number_of_partitions('callbacks.abc'); /* +1 partition (created by BGW) */
SELECT set_spawn_using_bgw('callbacks.abc', false);


SELECT append_range_partition('callbacks.abc');
SELECT prepend_range_partition('callbacks.abc');
SELECT add_range_partition('callbacks.abc', 501, 602);

SELECT drop_partitions('callbacks.abc');


/* set callback to be called on HASH partitions */
SELECT set_init_callback('callbacks.abc',
						 'callbacks.abc_on_part_created_callback(jsonb)');
SELECT create_hash_partitions('callbacks.abc', 'a', 5);

DROP TABLE callbacks.abc CASCADE;


/* test the temprary deletion of callback function */
CREATE TABLE callbacks.abc(a serial, b int);
SELECT set_init_callback('callbacks.abc',
						 'callbacks.abc_on_part_created_callback(jsonb)');
SELECT create_range_partitions('callbacks.abc', 'a', 1, 100, 2);

INSERT INTO callbacks.abc VALUES (201, 0); /* +1 new partition */

BEGIN;
DROP FUNCTION callbacks.abc_on_part_created_callback(jsonb);
INSERT INTO callbacks.abc VALUES (301, 0); /* +0 new partitions (ERROR) */
ROLLBACK;

INSERT INTO callbacks.abc VALUES (301, 0); /* +1 new partition */

DROP TABLE callbacks.abc CASCADE;


/* more complex test using rotation of tables */
CREATE TABLE callbacks.abc(a INT4 NOT NULL);
INSERT INTO callbacks.abc
	SELECT a FROM generate_series(1, 100) a;
SELECT create_range_partitions('callbacks.abc', 'a', 1, 10, 10);

CREATE OR REPLACE FUNCTION callbacks.rotation_callback(params jsonb)
RETURNS VOID AS
$$
DECLARE
	relation	regclass;
	parent_rel	regclass;
BEGIN
	parent_rel := concat(params->>'partition_schema', '.', params->>'parent')::regclass;

	-- drop "old" partitions
	FOR relation IN (SELECT partition FROM
						(SELECT partition, range_min::INT4 FROM pathman_partition_list
						 WHERE parent = parent_rel
						 ORDER BY range_min::INT4 DESC
						 OFFSET 4) t  -- remain 4 last partitions
					 ORDER BY range_min)
	LOOP
		RAISE NOTICE 'dropping partition %', relation;
		PERFORM drop_range_partition(relation);
	END LOOP;
END
$$ LANGUAGE plpgsql;

SELECT * FROM pathman_partition_list
WHERE parent = 'callbacks.abc'::REGCLASS
ORDER BY range_min::INT4;

SELECT set_init_callback('callbacks.abc',
						 'callbacks.rotation_callback(jsonb)');

INSERT INTO callbacks.abc VALUES (1000);
INSERT INTO callbacks.abc VALUES (1500);

SELECT * FROM pathman_partition_list
WHERE parent = 'callbacks.abc'::REGCLASS
ORDER BY range_min::INT4;



DROP TABLE callbacks.abc CASCADE;
DROP FUNCTION callbacks.abc_on_part_created_callback(jsonb);
DROP FUNCTION public.dummy_cb(jsonb);
DROP FUNCTION callbacks.rotation_callback(jsonb);
DROP SCHEMA callbacks;
DROP EXTENSION pg_pathman CASCADE;
