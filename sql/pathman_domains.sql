\set VERBOSITY terse

SET search_path = 'public';
CREATE EXTENSION pg_pathman;
CREATE SCHEMA domains;

CREATE DOMAIN domains.dom_test AS numeric CHECK (value < 1200);

CREATE TABLE domains.dom_table(val domains.dom_test NOT NULL);
INSERT INTO domains.dom_table SELECT generate_series(1, 999);

SELECT create_range_partitions('domains.dom_table', 'val', 1, 100);

EXPLAIN (COSTS OFF)
SELECT * FROM domains.dom_table
WHERE val < 250;

INSERT INTO domains.dom_table VALUES(1500);
INSERT INTO domains.dom_table VALUES(-10);

SELECT append_range_partition('domains.dom_table');
SELECT prepend_range_partition('domains.dom_table');
SELECT merge_range_partitions('domains.dom_table_1', 'domains.dom_table_2');
SELECT split_range_partition('domains.dom_table_1', 50);

INSERT INTO domains.dom_table VALUES(1101);

EXPLAIN (COSTS OFF)
SELECT * FROM domains.dom_table
WHERE val < 450;


SELECT * FROM pathman_partition_list
ORDER BY range_min::INT, range_max::INT;


SELECT drop_partitions('domains.dom_table');
SELECT create_hash_partitions('domains.dom_table', 'val', 5);

SELECT * FROM pathman_partition_list
ORDER BY "partition"::TEXT;


DROP TABLE domains.dom_table CASCADE;
DROP DOMAIN domains.dom_test CASCADE;
DROP SCHEMA domains;
DROP EXTENSION pg_pathman CASCADE;
