\set VERBOSITY terse

CREATE EXTENSION pg_pathman;

CREATE TABLE abc (id SERIAL);
CREATE TABLE xxx (id SERIAL);

SELECT create_range_partitions('abc', 'id', 1, 100, 3);
SELECT create_fk('xxx', 'id', 'abc', 'id');

INSERT INTO xxx VALUES (1);

INSERT INTO abc VALUES (1);
INSERT INTO xxx VALUES (1);

UPDATE abc SET id=2 WHERE id=1;
UPDATE xxx SET id=2 WHERE id=1;

DROP EXTENSION pg_pathman;
