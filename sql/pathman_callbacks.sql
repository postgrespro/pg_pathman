\set VERBOSITY terse

CREATE EXTENSION pg_pathman;
CREATE SCHEMA callbacks;

/* Check callbacks */
CREATE TABLE callbacks.log(id serial, message text);

CREATE OR REPLACE FUNCTION callbacks.abc_on_part_created_range_callback(
	args JSONB)
RETURNS VOID AS $$
DECLARE
	start_value	TEXT := args->>'start';
	end_value	TEXT := args->'end';
BEGIN
	INSERT INTO callbacks.log(message)
	VALUES (start_value || '-' || end_value);
END
$$ language plpgsql;


CREATE OR REPLACE FUNCTION callbacks.abc_on_part_created_hash_callback(
	args JSONB)
RETURNS VOID AS $$
BEGIN
	RAISE WARNING 'callback: partition %', args->'partition';
END
$$ language plpgsql;


/* set callback to be called on RANGE partitions */
CREATE TABLE callbacks.abc(a serial, b int);
SELECT create_range_partitions('callbacks.abc', 'a', 1, 100, 2);

SELECT set_part_init_callback('callbacks.abc',
							  'callbacks.abc_on_part_created_range_callback');

INSERT INTO callbacks.abc VALUES (123, 1);
INSERT INTO callbacks.abc VALUES (223, 1);

SELECT append_range_partition('callbacks.abc');
SELECT prepend_range_partition('callbacks.abc');
SELECT add_range_partition('callbacks.abc', 401, 502);

SELECT message FROM callbacks.log ORDER BY id;

SELECT drop_partitions('callbacks.abc');


/* set callback to be called on HASH partitions */
SELECT set_part_init_callback('callbacks.abc',
							  'callbacks.abc_on_part_created_hash_callback');
SELECT create_hash_partitions('callbacks.abc', 'a', 5);


DROP SCHEMA callbacks CASCADE;
DROP EXTENSION pg_pathman CASCADE;
