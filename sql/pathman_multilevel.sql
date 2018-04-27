\set VERBOSITY terse

SET search_path = 'public';
CREATE EXTENSION pg_pathman;
CREATE SCHEMA multi;


/* Check that multilevel is prohibited */
CREATE TABLE multi.test(key int NOT NULL);
SELECT create_hash_partitions('multi.test', 'key', 3);
SELECT create_hash_partitions('multi.test_1', 'key', 3);
DROP TABLE multi.test CASCADE;


/* Attach partitioned subtree to 'abc' */
CREATE TABLE multi.abc (val int NOT NULL);
CREATE TABLE multi.def (LIKE multi.abc);
SELECT create_hash_partitions('multi.def', 'val', 2);
ALTER TABLE multi.def INHERIT multi.abc;

/*
 * Although multilevel partitioning is not supported,
 * we must make sure that pg_pathman won't add
 * duplicate relations to the final plan.
 */
EXPLAIN (COSTS OFF) TABLE multi.abc;


DROP SCHEMA multi CASCADE;
DROP EXTENSION pg_pathman;
