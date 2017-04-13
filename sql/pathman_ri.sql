\set VERBOSITY terse

CREATE EXTENSION pg_pathman;

CREATE TABLE abc (id SERIAL);
CREATE TABLE xxx (id SERIAL, abc_id INT);

/* Cannot create FK if PK table isn't partitioned by pg_pathman */
SELECT create_fk('xxx', 'abc_id', 'abc');

/* Cannot create FK if there are no unique indexes */
SELECT create_range_partitions('abc', 'id', 1, 100, 3);
SELECT create_fk('xxx', 'abc_id', 'abc');

/* Create index on parent but not on partitions. Expect an error */
CREATE UNIQUE INDEX on abc(id);
SELECT create_fk('xxx', 'abc_id', 'abc');

/* Recreate partitioning setup with unique indexes */
SELECT drop_partitions('abc');
CREATE UNIQUE INDEX on abc(id);
SELECT create_range_partitions('abc', 'id', 1, 100, 3);

/* Existing row should violate FK constraint */
INSERT INTO xxx (abc_id) VALUES (1);
SELECT create_fk('xxx', 'abc_id', 'abc');

/* Expect successful execution */
DELETE FROM xxx;
SELECT create_fk('xxx', 'abc_id', 'abc');

/* PK violation */
INSERT INTO xxx (id, abc_id) VALUES (100, 1);

/* No violation */
INSERT INTO abc VALUES (1);
INSERT INTO xxx (id, abc_id) VALUES (100, 1);

/* Restrict an update of FK table */
UPDATE xxx SET abc_id=2 WHERE id=100;

/* No restriction */
INSERT INTO abc VALUES (2);
UPDATE xxx SET abc_id=2 WHERE id=100;

/* Restrict an update or delete on PK table */
UPDATE abc SET id = 3 WHERE id = 2;
DELETE FROM abc WHERE id = 2;

/* Unique indexes cannot be dropped */
DROP INDEX abc_1_id_idx;

/* Add partition */
SELECT append_range_partition('abc');
INSERT INTO abc VALUES (350);
INSERT INTO xxx (abc_id) VALUES (350);
DROP TABLE abc_4;

DROP EXTENSION pg_pathman CASCADE;
