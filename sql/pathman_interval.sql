\set VERBOSITY terse

SET search_path = 'public';
CREATE EXTENSION pg_pathman;
CREATE SCHEMA test_interval;



/* Range partitions for INT2 type */
CREATE TABLE test_interval.abc (id INT2 NOT NULL);
SELECT create_range_partitions('test_interval.abc', 'id', 0, 100, 2);
SELECT set_interval('test_interval.abc', NULL::INT2);

/* pg_pathman shouldn't be able to create a new partition */
INSERT INTO test_interval.abc VALUES (250);

/* Set a trivial interval */
SELECT set_interval('test_interval.abc', 0);

/* Set a negative interval */
SELECT set_interval('test_interval.abc', -100);

/* We also shouldn't be able to set a trivial interval directly */
UPDATE pathman_config SET range_interval = '0'
WHERE partrel = 'test_interval.abc'::REGCLASS;

/* Set a normal interval */
SELECT set_interval('test_interval.abc', 1000);
INSERT INTO test_interval.abc VALUES (250);
SELECT partrel, range_interval FROM pathman_config;

DROP TABLE test_interval.abc CASCADE;


/* Range partitions for INT4 type */
CREATE TABLE test_interval.abc (id INT4 NOT NULL);
SELECT create_range_partitions('test_interval.abc', 'id', 0, 100, 2);
SELECT set_interval('test_interval.abc', NULL::INT4);

/* pg_pathman shouldn't be able to create a new partition */
INSERT INTO test_interval.abc VALUES (250);

/* Set a trivial interval */
SELECT set_interval('test_interval.abc', 0);

/* Set a negative interval */
SELECT set_interval('test_interval.abc', -100);

/* We also shouldn't be able to set a trivial interval directly */
UPDATE pathman_config SET range_interval = '0'
WHERE partrel = 'test_interval.abc'::REGCLASS;

/* Set a normal interval */
SELECT set_interval('test_interval.abc', 1000);
INSERT INTO test_interval.abc VALUES (250);
SELECT partrel, range_interval FROM pathman_config;

DROP TABLE test_interval.abc CASCADE;


/* Range partitions for INT8 type */
CREATE TABLE test_interval.abc (id INT8 NOT NULL);
SELECT create_range_partitions('test_interval.abc', 'id', 0, 100, 2);
SELECT set_interval('test_interval.abc', NULL::INT8);

/* pg_pathman shouldn't be able to create a new partition */
INSERT INTO test_interval.abc VALUES (250);

/* Set a trivial interval */
SELECT set_interval('test_interval.abc', 0);

/* Set a negative interval */
SELECT set_interval('test_interval.abc', -100);

/* We also shouldn't be able to set a trivial interval directly */
UPDATE pathman_config SET range_interval = '0'
WHERE partrel = 'test_interval.abc'::REGCLASS;

/* Set a normal interval */
SELECT set_interval('test_interval.abc', 1000);
INSERT INTO test_interval.abc VALUES (250);
SELECT partrel, range_interval FROM pathman_config;

DROP TABLE test_interval.abc CASCADE;


/* Range partitions for DATE type */
CREATE TABLE test_interval.abc (dt DATE NOT NULL);
SELECT create_range_partitions('test_interval.abc', 'dt',
							   '2016-01-01'::DATE, '1 day'::INTERVAL, 2);
SELECT set_interval('test_interval.abc', NULL::INTERVAL);

/* Set a trivial interval */
SELECT set_interval('test_interval.abc', '1 second'::INTERVAL);

/* Set a normal interval */
SELECT set_interval('test_interval.abc', '1 month'::INTERVAL);

SELECT partrel, range_interval FROM pathman_config;

DROP TABLE test_interval.abc CASCADE;


/* Range partitions for FLOAT4 type */
CREATE TABLE test_interval.abc (x FLOAT4 NOT NULL);
SELECT create_range_partitions('test_interval.abc', 'x', 0, 100, 2);
SELECT set_interval('test_interval.abc', NULL::FLOAT4);

/* Set a trivial interval */
SELECT set_interval('test_interval.abc', 0);

/* Set NaN float as interval */
SELECT set_interval('test_interval.abc', 'NaN'::FLOAT4);

/* Set INF float as interval */
SELECT set_interval('test_interval.abc', 'Infinity'::FLOAT4);

/* Set a normal interval */
SELECT set_interval('test_interval.abc', 100);

DROP TABLE test_interval.abc CASCADE;


/* Range partitions for FLOAT8 type */
CREATE TABLE test_interval.abc (x FLOAT8 NOT NULL);
SELECT create_range_partitions('test_interval.abc', 'x', 0, 100, 2);
SELECT set_interval('test_interval.abc', NULL::FLOAT8);

/* Set a trivial interval */
SELECT set_interval('test_interval.abc', 0);

/* Set NaN float as interval */
SELECT set_interval('test_interval.abc', 'NaN'::FLOAT8);

/* Set INF float as interval */
SELECT set_interval('test_interval.abc', 'Infinity'::FLOAT8);

/* Set a normal interval */
SELECT set_interval('test_interval.abc', 100);

DROP TABLE test_interval.abc CASCADE;


/* Range partitions for NUMERIC type */
CREATE TABLE test_interval.abc (x NUMERIC NOT NULL);
SELECT create_range_partitions('test_interval.abc', 'x', 0, 100, 2);
SELECT set_interval('test_interval.abc', NULL::NUMERIC);

/* Set a trivial interval */
SELECT set_interval('test_interval.abc', 0);

/* Set NaN numeric as interval */
SELECT set_interval('test_interval.abc', 'NaN'::NUMERIC);

/* Set a normal interval */
SELECT set_interval('test_interval.abc', 100);

DROP TABLE test_interval.abc CASCADE;


/* Hash partitioned table shouldn't accept any interval value */
CREATE TABLE test_interval.abc (id SERIAL);
SELECT create_hash_partitions('test_interval.abc', 'id', 3);
SELECT set_interval('test_interval.abc', 100);
SELECT set_interval('test_interval.abc', NULL::INTEGER);

DROP TABLE test_interval.abc CASCADE;



DROP SCHEMA test_interval;
DROP EXTENSION pg_pathman;
