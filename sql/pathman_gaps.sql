/*
 * Since 8edd0e794 (>= 12) Append nodes with single subplan are eliminated,
 * causing different output; pathman_gaps_1.out is the updated version.
 */
\set VERBOSITY terse

SET search_path = 'public';
CREATE EXTENSION pg_pathman;
CREATE SCHEMA gaps;



CREATE TABLE gaps.test_1(val INT8 NOT NULL);
SELECT create_range_partitions('gaps.test_1', 'val', 1, 10, 3);
DROP TABLE gaps.test_1_2;

CREATE TABLE gaps.test_2(val INT8 NOT NULL);
SELECT create_range_partitions('gaps.test_2', 'val', 1, 10, 5);
DROP TABLE gaps.test_2_3;

CREATE TABLE gaps.test_3(val INT8 NOT NULL);
SELECT create_range_partitions('gaps.test_3', 'val', 1, 10, 8);
DROP TABLE gaps.test_3_4;

CREATE TABLE gaps.test_4(val INT8 NOT NULL);
SELECT create_range_partitions('gaps.test_4', 'val', 1, 10, 11);
DROP TABLE gaps.test_4_4;
DROP TABLE gaps.test_4_5;



/* Check existing partitions */
SELECT * FROM pathman_partition_list ORDER BY parent, partition;



/* Pivot values */
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_1 WHERE val =  11;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_1 WHERE val =  16;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_1 WHERE val =  21;

EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_1 WHERE val <  11;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_1 WHERE val <= 11;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_1 WHERE val <  16;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_1 WHERE val <= 16;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_1 WHERE val <  21;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_1 WHERE val <= 21;

EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_1 WHERE val >  11;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_1 WHERE val >= 11;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_1 WHERE val >  16;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_1 WHERE val >= 16;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_1 WHERE val >  21;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_1 WHERE val >= 21;


/* Pivot values */
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_2 WHERE val =  21;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_2 WHERE val =  26;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_2 WHERE val =  31;

EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_2 WHERE val <  21;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_2 WHERE val <= 21;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_2 WHERE val <  26;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_2 WHERE val <= 26;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_2 WHERE val <  31;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_2 WHERE val <= 31;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_2 WHERE val <  41;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_2 WHERE val <= 41;

EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_2 WHERE val >  11;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_2 WHERE val >= 11;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_2 WHERE val >  21;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_2 WHERE val >= 21;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_2 WHERE val >  26;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_2 WHERE val >= 26;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_2 WHERE val >  31;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_2 WHERE val >= 31;


/* Pivot values */
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_3 WHERE val =  31;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_3 WHERE val =  36;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_3 WHERE val =  41;

EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_3 WHERE val <  31;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_3 WHERE val <= 31;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_3 WHERE val <  36;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_3 WHERE val <= 36;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_3 WHERE val <  41;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_3 WHERE val <= 41;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_3 WHERE val <  51;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_3 WHERE val <= 51;

EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_3 WHERE val >  21;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_3 WHERE val >= 21;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_3 WHERE val >  31;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_3 WHERE val >= 31;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_3 WHERE val >  36;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_3 WHERE val >= 36;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_3 WHERE val >  41;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_3 WHERE val >= 41;


/* Pivot values */
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val =  31;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val =  36;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val =  41;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val =  46;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val =  51;

EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val <  31;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val <= 31;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val <  36;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val <= 36;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val <  41;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val <= 41;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val <  46;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val <= 46;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val <  51;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val <= 51;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val <  61;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val <= 61;

EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val >  21;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val >= 21;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val >  31;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val >= 31;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val >  36;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val >= 36;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val >  41;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val >= 41;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val >  46;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val >= 46;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val >  51;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val >= 51;



DROP SCHEMA gaps CASCADE;
DROP EXTENSION pg_pathman;
