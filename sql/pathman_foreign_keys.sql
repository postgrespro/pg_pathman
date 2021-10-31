\set VERBOSITY terse

SET search_path = 'public';
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


/* Try to partition table that's being referenced */
CREATE TABLE fkeys.messages(
	id			SERIAL PRIMARY KEY,
	msg			TEXT);

CREATE TABLE fkeys.replies(
	id			SERIAL PRIMARY KEY,
	message_id	INTEGER REFERENCES fkeys.messages(id),
	msg			TEXT);

INSERT INTO fkeys.messages SELECT g, md5(g::text) FROM generate_series(1, 10) as g;
INSERT INTO fkeys.replies SELECT g, g, md5(g::text) FROM generate_series(1, 10) as g;

SELECT create_range_partitions('fkeys.messages', 'id', 1, 100, 2); /* not ok */

ALTER TABLE fkeys.replies DROP CONSTRAINT replies_message_id_fkey;

SELECT create_range_partitions('fkeys.messages', 'id', 1, 100, 2); /* ok */
EXPLAIN (COSTS OFF) SELECT * FROM fkeys.messages;

DROP TABLE fkeys.messages, fkeys.replies CASCADE;



DROP TABLE fkeys.test_fkey CASCADE;
DROP TABLE fkeys.test_ref CASCADE;
DROP SCHEMA fkeys;
DROP EXTENSION pg_pathman CASCADE;
