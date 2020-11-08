/*
 * Since 55a1954da16 and 6ef77cf46e8 (>= 13) output of EXPLAIN was changed,
 * now it includes aliases for inherited tables.
 */

\set VERBOSITY terse

SET search_path = 'public';
CREATE EXTENSION pg_pathman;
CREATE SCHEMA test_inserts;


/* create a partitioned table */
CREATE TABLE test_inserts.storage(a INT4, b INT4 NOT NULL, c NUMERIC, d TEXT);
INSERT INTO test_inserts.storage SELECT i * 2, i, i, i::text FROM generate_series(1, 100) i;
CREATE UNIQUE INDEX ON test_inserts.storage(a);
SELECT create_range_partitions('test_inserts.storage', 'b', 1, 10);

/* attach before and after insertion triggers to partitioned table */
CREATE OR REPLACE FUNCTION test_inserts.print_cols_before_change() RETURNS TRIGGER AS $$
BEGIN
	RAISE NOTICE 'BEFORE INSERTION TRIGGER ON TABLE % HAS EXPIRED. INSERTED ROW: %', tg_table_name, new;
	RETURN new;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION test_inserts.print_cols_after_change() RETURNS TRIGGER AS $$
BEGIN
	RAISE NOTICE 'AFTER INSERTION TRIGGER ON TABLE % HAS EXPIRED. INSERTED ROW: %', tg_table_name, new;
	RETURN new;
END;
$$ LANGUAGE plpgsql;

/* set triggers on existing first partition and new generated partitions */
CREATE TRIGGER print_new_row_before_insert BEFORE INSERT ON test_inserts.storage_1
	FOR EACH ROW EXECUTE PROCEDURE test_inserts.print_cols_before_change();

CREATE TRIGGER print_new_row_after_insert AFTER INSERT ON test_inserts.storage_1
	FOR EACH ROW EXECUTE PROCEDURE test_inserts.print_cols_after_change();

/* set partition init callback that will add triggers to partitions */
CREATE OR REPLACE FUNCTION test_inserts.set_triggers(args jsonb) RETURNS VOID AS $$
BEGIN
	EXECUTE format('create trigger print_new_row_before_insert before insert on %s.%s
						for each row execute procedure test_inserts.print_cols_before_change();',
				   args->>'partition_schema', args->>'partition');
	EXECUTE format('create trigger print_new_row_after_insert after insert on %s.%s
						for each row execute procedure test_inserts.print_cols_after_change();',
				   args->>'partition_schema', args->>'partition');
END;
$$ LANGUAGE plpgsql;
SELECT set_init_callback('test_inserts.storage', 'test_inserts.set_triggers(jsonb)');


/* we don't support ON CONLICT */
INSERT INTO test_inserts.storage VALUES(0, 0, 0, 'UNSUPPORTED_1')
ON CONFLICT (a) DO UPDATE SET a = 3;
INSERT INTO test_inserts.storage VALUES(0, 0, 0, 'UNSUPPORTED_2')
ON CONFLICT (a) DO NOTHING;


/* implicitly prepend a partition (no columns have been dropped yet) */
INSERT INTO test_inserts.storage VALUES(0, 0, 0, 'PREPEND.') RETURNING *;
SELECT * FROM test_inserts.storage_11;

INSERT INTO test_inserts.storage VALUES(1, 0, 0, 'PREPEND..') RETURNING tableoid::regclass;
SELECT * FROM test_inserts.storage_11;

INSERT INTO test_inserts.storage VALUES(3, 0, 0, 'PREPEND...') RETURNING a + b / 3;
SELECT * FROM test_inserts.storage_11;

/* cause an unique index conflict (a = 0) */
INSERT INTO test_inserts.storage VALUES(0, 0, 0, 'CONFLICT') RETURNING *;


/* drop first column */
ALTER TABLE test_inserts.storage DROP COLUMN a CASCADE;


/* will have 3 columns (b, c, d) */
SELECT append_range_partition('test_inserts.storage');
INSERT INTO test_inserts.storage (b, c, d) VALUES (101, 17, '3 cols!');
SELECT * FROM test_inserts.storage_12; /* direct access */
SELECT * FROM test_inserts.storage WHERE b > 100; /* via parent */

/* spawn a new partition (b, c, d) */
INSERT INTO test_inserts.storage (b, c, d) VALUES (111, 17, '3 cols as well!');
SELECT * FROM test_inserts.storage_13; /* direct access */
SELECT * FROM test_inserts.storage WHERE b > 110; /* via parent */


/* column 'a' has been dropped */
INSERT INTO test_inserts.storage VALUES(111, 0, 'DROP_COL_1.') RETURNING *, 17;
INSERT INTO test_inserts.storage VALUES(111, 0, 'DROP_COL_1..') RETURNING tableoid::regclass;
INSERT INTO test_inserts.storage VALUES(111, 0, 'DROP_COL_1...') RETURNING b * 2, b;


/* drop third column */
ALTER TABLE test_inserts.storage DROP COLUMN c CASCADE;


/* will have 2 columns (b, d) */
SELECT append_range_partition('test_inserts.storage');
INSERT INTO test_inserts.storage (b, d) VALUES (121, '2 cols!');
SELECT * FROM test_inserts.storage_14; /* direct access */
SELECT * FROM test_inserts.storage WHERE b > 120; /* via parent */


/* column 'c' has been dropped */
INSERT INTO test_inserts.storage VALUES(121, 'DROP_COL_2.') RETURNING *;
INSERT INTO test_inserts.storage VALUES(121, 'DROP_COL_2..') RETURNING tableoid::regclass;
INSERT INTO test_inserts.storage VALUES(121, 'DROP_COL_2...') RETURNING d || '0_0', b * 3;



INSERT INTO test_inserts.storage VALUES(121, 'query_1')
RETURNING (SELECT 1);

INSERT INTO test_inserts.storage VALUES(121, 'query_2')
RETURNING (SELECT generate_series(1, 10) LIMIT 1);

INSERT INTO test_inserts.storage VALUES(121, 'query_3')
RETURNING (SELECT get_partition_key('test_inserts.storage'));

INSERT INTO test_inserts.storage VALUES(121, 'query_4')
RETURNING 1, 2, 3, 4;



/* show number of columns in each partition */
SELECT partition, range_min, range_max, count(partition)
FROM pathman_partition_list JOIN pg_attribute ON partition = attrelid
WHERE attnum > 0
GROUP BY partition, range_min, range_max
ORDER BY range_min::INT4;


/* check the data */
SELECT *, tableoid::regclass FROM test_inserts.storage ORDER BY b, d;

/* drop data */
TRUNCATE test_inserts.storage;


/* one more time! */
INSERT INTO test_inserts.storage (b, d) SELECT i, i FROM generate_series(-2, 120) i;
SELECT *, tableoid::regclass FROM test_inserts.storage ORDER BY b, d;

/* drop data */
TRUNCATE test_inserts.storage;


/* add new column */
ALTER TABLE test_inserts.storage ADD COLUMN e INT8 NOT NULL;


/* one more time! x2 */
INSERT INTO test_inserts.storage (b, d, e) SELECT i, i, i FROM generate_series(-2, 120) i;
SELECT *, tableoid::regclass FROM test_inserts.storage ORDER BY b, d;

/* drop data */
TRUNCATE test_inserts.storage;


/* now test RETURNING list using our new column 'e' */
INSERT INTO test_inserts.storage (b, d, e) SELECT i, i, i
FROM generate_series(-2, 130, 5) i
RETURNING e * 2, b, tableoid::regclass;


/* test EXPLAIN (VERBOSE) - for PartitionFilter's targetlists */
EXPLAIN (VERBOSE, COSTS OFF)
INSERT INTO test_inserts.storage (b, d, e) SELECT i, i, i
FROM generate_series(1, 10) i
RETURNING e * 2, b, tableoid::regclass;

EXPLAIN (VERBOSE, COSTS OFF)
INSERT INTO test_inserts.storage (d, e) SELECT i, i
FROM generate_series(1, 10) i;

EXPLAIN (VERBOSE, COSTS OFF)
INSERT INTO test_inserts.storage (b) SELECT i
FROM generate_series(1, 10) i;

EXPLAIN (VERBOSE, COSTS OFF)
INSERT INTO test_inserts.storage (b, d, e) SELECT b, d, e
FROM test_inserts.storage;

EXPLAIN (VERBOSE, COSTS OFF)
INSERT INTO test_inserts.storage (b, d) SELECT b, d
FROM test_inserts.storage;

EXPLAIN (VERBOSE, COSTS OFF)
INSERT INTO test_inserts.storage (b) SELECT b
FROM test_inserts.storage;


/* test gap case (missing partition in between) */
CREATE TABLE test_inserts.test_gap(val INT NOT NULL);
INSERT INTO test_inserts.test_gap SELECT generate_series(1, 30);
SELECT create_range_partitions('test_inserts.test_gap', 'val', 1, 10);
DROP TABLE test_inserts.test_gap_2; /* make a gap */
INSERT INTO test_inserts.test_gap VALUES(15); /* not ok */
DROP TABLE test_inserts.test_gap CASCADE;


/* test a few "special" ONLY queries used in pg_repack */
CREATE TABLE test_inserts.test_special_only(val INT NOT NULL);
INSERT INTO test_inserts.test_special_only SELECT generate_series(1, 30);
SELECT create_hash_partitions('test_inserts.test_special_only', 'val', 4);

/* create table as select only */
CREATE TABLE test_inserts.special_1 AS SELECT * FROM ONLY test_inserts.test_special_only;
SELECT count(*) FROM test_inserts.special_1;
DROP TABLE test_inserts.special_1;

/* insert into ... select only */
CREATE TABLE test_inserts.special_2 AS SELECT * FROM ONLY test_inserts.test_special_only WITH NO DATA;
INSERT INTO test_inserts.special_2 SELECT * FROM ONLY test_inserts.test_special_only;
SELECT count(*) FROM test_inserts.special_2;
DROP TABLE test_inserts.special_2;

DROP TABLE test_inserts.test_special_only CASCADE;


DROP SCHEMA test_inserts CASCADE;
DROP EXTENSION pg_pathman CASCADE;
