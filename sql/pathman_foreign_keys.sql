\set VERBOSITY terse

CREATE EXTENSION pg_pathman;
CREATE SCHEMA fkeys;

/* Check primary keys generation */
CREATE TABLE fkeys.test_ref(comment TEXT UNIQUE);
INSERT INTO fkeys.test_ref VALUES('test');

CREATE TABLE fkeys.test_fkey(
	id			INT NOT NULL,
	comment		TEXT,
	FOREIGN KEY (comment) REFERENCES fkeys.test_ref(comment));

INSERT INTO fkeys.test_fkey SELECT generate_series(1, 1000), 'test';

SELECT create_range_partitions('fkeys.test_fkey', 'id', 1, 100);
INSERT INTO fkeys.test_fkey VALUES(1, 'wrong');
INSERT INTO fkeys.test_fkey VALUES(1, 'test');
SELECT drop_partitions('fkeys.test_fkey');

SELECT create_hash_partitions('fkeys.test_fkey', 'id', 10);
INSERT INTO fkeys.test_fkey VALUES(1, 'wrong');
INSERT INTO fkeys.test_fkey VALUES(1, 'test');
SELECT drop_partitions('fkeys.test_fkey');


DROP SCHEMA fkeys CASCADE;
DROP EXTENSION pg_pathman CASCADE;
