\set VERBOSITY terse

CREATE EXTENSION pg_pathman;

/*
 * Test COPY
 */
CREATE SCHEMA copy_stmt_hooking;

CREATE TABLE copy_stmt_hooking.test(
	val int not null,
	comment text,
	c3 int,
	c4 int);
INSERT INTO copy_stmt_hooking.test SELECT generate_series(1, 20), 'comment';
CREATE INDEX ON copy_stmt_hooking.test(val);


/* test for RANGE partitioning */
SELECT create_range_partitions('copy_stmt_hooking.test', 'val', 1, 5);

/* perform VACUUM */
VACUUM FULL copy_stmt_hooking.test;
VACUUM FULL copy_stmt_hooking.test_1;
VACUUM FULL copy_stmt_hooking.test_2;
VACUUM FULL copy_stmt_hooking.test_3;
VACUUM FULL copy_stmt_hooking.test_4;

/* COPY TO */
COPY copy_stmt_hooking.test TO stdout;
\copy copy_stmt_hooking.test to stdout (format csv)
\copy copy_stmt_hooking.test(comment) to stdout

/* DELETE ROWS, COPY FROM */
DELETE FROM copy_stmt_hooking.test;
COPY copy_stmt_hooking.test FROM stdin;
1	test_1	0	0
6	test_2	0	0
7	test_2	0	0
11	test_3	0	0
16	test_4	0	0
\.
SELECT count(*) FROM ONLY copy_stmt_hooking.test;
SELECT *, tableoid::REGCLASS FROM copy_stmt_hooking.test ORDER BY val;

/* perform VACUUM */
VACUUM FULL copy_stmt_hooking.test;
VACUUM FULL copy_stmt_hooking.test_1;
VACUUM FULL copy_stmt_hooking.test_2;
VACUUM FULL copy_stmt_hooking.test_3;
VACUUM FULL copy_stmt_hooking.test_4;

/* COPY FROM (specified columns) */
COPY copy_stmt_hooking.test (val) TO stdout;
COPY copy_stmt_hooking.test (val, comment) TO stdout;
COPY copy_stmt_hooking.test (c3, val, comment) TO stdout;
COPY copy_stmt_hooking.test (val, comment, c3, c4) TO stdout;

/* COPY TO (partition does not exist, NOT allowed to create partitions) */
SET pg_pathman.enable_auto_partition = OFF;
COPY copy_stmt_hooking.test FROM stdin;
21	test_no_part	0	0
\.
SELECT * FROM copy_stmt_hooking.test WHERE val > 20;

/* COPY TO (partition does not exist, allowed to create partitions) */
SET pg_pathman.enable_auto_partition = ON;
COPY copy_stmt_hooking.test FROM stdin;
21	test_no_part	0	0
\.
SELECT * FROM copy_stmt_hooking.test WHERE val > 20;

/* COPY FROM (partitioned column is not specified) */
COPY copy_stmt_hooking.test(comment) FROM stdin;
test_no_part
\.

/* COPY FROM (we don't support FREEZE) */
COPY copy_stmt_hooking.test FROM stdin WITH (FREEZE);


/* Drop column (make use of 'tuple_map') */
ALTER TABLE copy_stmt_hooking.test DROP COLUMN comment;


/* create new partition */
SELECT get_number_of_partitions('copy_stmt_hooking.test');
INSERT INTO copy_stmt_hooking.test (val, c3, c4) VALUES (26, 1, 2);
SELECT get_number_of_partitions('copy_stmt_hooking.test');

/* check number of columns in 'test' */
SELECT count(*) FROM pg_attribute
WHERE attnum > 0 AND attrelid = 'copy_stmt_hooking.test'::REGCLASS;

/* check number of columns in 'test_6' */
SELECT count(*) FROM pg_attribute
WHERE attnum > 0 AND attrelid = 'copy_stmt_hooking.test_6'::REGCLASS;


/* COPY FROM (test transformed tuples) */
COPY copy_stmt_hooking.test (val, c3, c4) TO stdout;

/* COPY TO (insert into table with dropped column) */
COPY copy_stmt_hooking.test(val, c3, c4) FROM stdin;
2	1	2
\.

/* COPY TO (insert into table without dropped column) */
COPY copy_stmt_hooking.test(val, c3, c4) FROM stdin;
27	1	2
\.

/* check tuples from last partition (without dropped column) */
SELECT *, tableoid::REGCLASS FROM copy_stmt_hooking.test ORDER BY val;


/* drop modified table */
DROP TABLE copy_stmt_hooking.test CASCADE;


/* create table again */
CREATE TABLE copy_stmt_hooking.test(
	val int not null,
	comment text,
	c3 int,
	c4 int);
CREATE INDEX ON copy_stmt_hooking.test(val);


/* test for HASH partitioning */
SELECT create_hash_partitions('copy_stmt_hooking.test', 'val', 5);

/* DELETE ROWS, COPY FROM */
DELETE FROM copy_stmt_hooking.test;
COPY copy_stmt_hooking.test FROM stdin;
1	hash_1	0	0
6	hash_2	0	0
\.
SELECT count(*) FROM ONLY copy_stmt_hooking.test;
SELECT * FROM copy_stmt_hooking.test ORDER BY val;

DROP SCHEMA copy_stmt_hooking CASCADE;


/*
 * Test auto check constraint renaming
 */
CREATE SCHEMA rename;

CREATE TABLE rename.test(a serial, b int);
SELECT create_hash_partitions('rename.test', 'a', 3);
ALTER TABLE rename.test_0 RENAME TO test_one;
/* We expect to find check constraint renamed as well */
\d+ rename.test_one

/* Generates check constraint for relation */
CREATE OR REPLACE FUNCTION add_constraint(rel regclass, att text)
RETURNS VOID AS $$
declare
	constraint_name text := build_check_constraint_name(rel, 'a');
BEGIN
	EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %s CHECK (a < 100);',
				   rel, constraint_name);
END
$$
LANGUAGE plpgsql;

/*
 * Check that it doesn't affect regular inherited
 * tables that aren't managed by pg_pathman
 */
CREATE TABLE rename.test_inh (LIKE rename.test INCLUDING ALL);
CREATE TABLE rename.test_inh_1 (LIKE rename.test INCLUDING ALL);
ALTER TABLE rename.test_inh_1 INHERIT rename.test_inh;
SELECT add_constraint('rename.test_inh_1', 'a');
ALTER TABLE rename.test_inh_1 RENAME TO test_inh_one;
\d+ rename.test_inh_one

/* Check that plain tables are not affected too */
CREATE TABLE rename.plain_test(a serial, b int);
ALTER TABLE rename.plain_test RENAME TO plain_test_renamed;
SELECT add_constraint('rename.plain_test_renamed', 'a');
\d+ rename.plain_test_renamed
ALTER TABLE rename.plain_test_renamed RENAME TO plain_test;
\d+ rename.plain_test

DROP SCHEMA rename CASCADE;

DROP EXTENSION pg_pathman;
