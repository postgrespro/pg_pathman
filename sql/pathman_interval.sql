\set VERBOSITY terse
CREATE EXTENSION pg_pathman;

/* Range partitions for INTEGER type */
CREATE TABLE abc (id SERIAL);
SELECT create_range_partitions('abc', 'id', 0, 100, 2);
SELECT set_interval('abc', NULL::INTEGER);
/* pg_pathman shouldn't be able to create a new partition */
INSERT INTO abc VALUES (250);
/* Set a trivial interval */
SELECT set_interval('abc', 0);
/* Set a negative interval */
SELECT set_interval('abc', -100);
/* We also shouldn't be able to set a trivial interval directly in pathman_config table */
UPDATE pathman_config SET range_interval = '0' WHERE partrel = 'abc'::REGCLASS;
/* Set a normal interval */
SELECT set_interval('abc', 1000);
INSERT INTO abc VALUES (250);
SELECT * FROM pathman_config;
DROP TABLE abc CASCADE;

/* Range partitions for DATE type */
CREATE TABLE abc (dt DATE NOT NULL);
SELECT create_range_partitions('abc', 'dt', '2016-01-01'::DATE, '1 day'::INTERVAL, 2);
SELECT set_interval('abc', NULL::INTERVAL);
/* Set a trivial interval */
SELECT set_interval('abc', '1 second'::INTERVAL);
/* Set a normal interval */
SELECT set_interval('abc', '1 month'::INTERVAL);
SELECT * FROM pathman_config;
DROP TABLE abc CASCADE;

/* Range partitions for FLOAT4 type */
CREATE TABLE abc (x FLOAT4 NOT NULL);
SELECT create_range_partitions('abc', 'x', 0, 100, 2);
SELECT set_interval('abc', NULL::FLOAT4);
/* Set a trivial interval */
SELECT set_interval('abc', 0);
/* Set NaN float as interval */
SELECT set_interval('abc', 'NaN'::FLOAT4);
/* Set INF float as interval */
SELECT set_interval('abc', 'Infinity'::FLOAT4);
/* Set a normal interval */
SELECT set_interval('abc', 100);
DROP TABLE abc CASCADE;

/* Range partitions for FLOAT8 type */
CREATE TABLE abc (x FLOAT8 NOT NULL);
SELECT create_range_partitions('abc', 'x', 0, 100, 2);
SELECT set_interval('abc', NULL::FLOAT8);
/* Set a trivial interval */
SELECT set_interval('abc', 0);
/* Set NaN float as interval */
SELECT set_interval('abc', 'NaN'::FLOAT8);
/* Set INF float as interval */
SELECT set_interval('abc', 'Infinity'::FLOAT8);
/* Set a normal interval */
SELECT set_interval('abc', 100);
DROP TABLE abc CASCADE;

/* Range partitions for NUMERIC type */
CREATE TABLE abc (x NUMERIC NOT NULL);
SELECT create_range_partitions('abc', 'x', 0, 100, 2);
SELECT set_interval('abc', NULL::NUMERIC);
/* Set a trivial interval */
SELECT set_interval('abc', 0);
/* Set NaN numeric as interval */
SELECT set_interval('abc', 'NaN'::NUMERIC);
/* Set a normal interval */
SELECT set_interval('abc', 100);
DROP TABLE abc CASCADE;

/* Hash partitioned table shouldn't accept any interval value */
CREATE TABLE abc (id SERIAL);
SELECT create_hash_partitions('abc', 'id', 3);
SELECT set_interval('abc', 100);
SELECT set_interval('abc', NULL::INTEGER);
DROP TABLE abc CASCADE;

DROP EXTENSION pg_pathman;
