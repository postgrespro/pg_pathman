\set VERBOSITY terse

CREATE EXTENSION pg_pathman;
CREATE SCHEMA copy_stmt_hooking;


CREATE TABLE copy_stmt_hooking.test(val int not null, comment text);
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
1	test_1
6	test_2
7	test_2
11	test_3
16	test_4
\.
SELECT count(*) FROM ONLY copy_stmt_hooking.test;
SELECT *, tableoid::REGCLASS FROM copy_stmt_hooking.test ORDER BY val;

/* COPY TO (partition does not exist, NOT allowed to create partitions) */
SET pg_pathman.enable_auto_partition = OFF;
COPY copy_stmt_hooking.test FROM stdin;
21	test_no_part
\.
SELECT * FROM copy_stmt_hooking.test WHERE val > 20;

/* COPY TO (partition does not exist, allowed to create partitions) */
SET pg_pathman.enable_auto_partition = ON;
COPY copy_stmt_hooking.test FROM stdin;
21	test_no_part
\.
SELECT * FROM copy_stmt_hooking.test WHERE val > 20;

/* COPY TO (partitioned column is not specified) */
COPY copy_stmt_hooking.test(comment) FROM stdin;
test_no_part
\.


/* delete all data */
SELECT drop_partitions('copy_stmt_hooking.test', true);


/* test for HASH partitioning */
SELECT create_hash_partitions('copy_stmt_hooking.test', 'val', 5);

/* DELETE ROWS, COPY FROM */
DELETE FROM copy_stmt_hooking.test;
COPY copy_stmt_hooking.test FROM stdin;
1	hash_1
6	hash_2
\.
SELECT count(*) FROM ONLY copy_stmt_hooking.test;
SELECT * FROM copy_stmt_hooking.test ORDER BY val;


DROP SCHEMA copy_stmt_hooking CASCADE;
DROP EXTENSION pg_pathman;
