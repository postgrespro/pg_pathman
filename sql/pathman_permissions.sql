\set VERBOSITY terse

SET search_path = 'public';
CREATE EXTENSION pg_pathman;
CREATE SCHEMA permissions;

CREATE ROLE pathman_user1 LOGIN;
CREATE ROLE pathman_user2 LOGIN;

GRANT USAGE, CREATE ON SCHEMA permissions TO pathman_user1;
GRANT USAGE, CREATE ON SCHEMA permissions TO pathman_user2;


/* Switch to #1 */
SET ROLE pathman_user1;
CREATE TABLE permissions.pathman_user1_table(id serial, a int);
INSERT INTO permissions.pathman_user1_table SELECT g, g FROM generate_series(1, 20) as g;

/* Should fail (can't SELECT) */
SET ROLE pathman_user2;
DO $$
BEGIN
    SELECT create_range_partitions('permissions.pathman_user1_table', 'id', 1, 10, 2);
EXCEPTION
    WHEN insufficient_privilege THEN
        RAISE NOTICE 'Insufficient priviliges';
END$$;

/* Grant SELECT to pathman_user2 */
SET ROLE pathman_user1;
GRANT SELECT ON permissions.pathman_user1_table TO pathman_user2;

/* Should fail (don't own parent) */
SET ROLE pathman_user2;
DO $$
BEGIN
    SELECT create_range_partitions('permissions.pathman_user1_table', 'id', 1, 10, 2);
EXCEPTION
    WHEN insufficient_privilege THEN
        RAISE NOTICE 'Insufficient priviliges';
END$$;

/* Should be ok */
SET ROLE pathman_user1;
SELECT create_range_partitions('permissions.pathman_user1_table', 'id', 1, 10, 2);

/* Should be able to see */
SET ROLE pathman_user2;
SELECT * FROM pathman_config;
SELECT * FROM pathman_config_params;

/* Should fail */
SET ROLE pathman_user2;
SELECT set_enable_parent('permissions.pathman_user1_table', true);
SELECT set_auto('permissions.pathman_user1_table', false);

/* Should fail */
SET ROLE pathman_user2;
DELETE FROM pathman_config
WHERE partrel = 'permissions.pathman_user1_table'::regclass;

/* No rights to insert, should fail */
SET ROLE pathman_user2;
DO $$
BEGIN
    INSERT INTO permissions.pathman_user1_table (id, a) VALUES (35, 0);
EXCEPTION
    WHEN insufficient_privilege THEN
        RAISE NOTICE 'Insufficient priviliges';
END$$;

/* No rights to create partitions (need INSERT privilege) */
SET ROLE pathman_user2;
SELECT prepend_range_partition('permissions.pathman_user1_table');

/* Allow pathman_user2 to create partitions */
SET ROLE pathman_user1;
GRANT INSERT ON permissions.pathman_user1_table TO pathman_user2;
GRANT UPDATE(a) ON permissions.pathman_user1_table TO pathman_user2; /* per-column ACL */

/* Should be able to prepend a partition */
SET ROLE pathman_user2;
SELECT prepend_range_partition('permissions.pathman_user1_table');
SELECT attname, attacl FROM pg_attribute
WHERE attrelid = (SELECT "partition" FROM pathman_partition_list
				  WHERE parent = 'permissions.pathman_user1_table'::REGCLASS
				  ORDER BY range_min::int ASC /* prepend */
				  LIMIT 1)
ORDER BY attname; /* check ACL for each column */

/* Have rights, should be ok (parent's ACL is shared by new children) */
SET ROLE pathman_user2;
INSERT INTO permissions.pathman_user1_table (id, a) VALUES (35, 0) RETURNING *;
SELECT relname, relacl FROM pg_class
WHERE oid = ANY (SELECT "partition" FROM pathman_partition_list
				 WHERE parent = 'permissions.pathman_user1_table'::REGCLASS
				 ORDER BY range_max::int DESC /* append */
				 LIMIT 3)
ORDER BY relname; /* we also check ACL for "pathman_user1_table_2" */

/* Try to drop partition, should fail */
DO $$
BEGIN
    SELECT drop_range_partition('permissions.pathman_user1_table_4');
EXCEPTION
    WHEN insufficient_privilege THEN
        RAISE NOTICE 'Insufficient priviliges';
END$$;

/* Disable automatic partition creation */
SET ROLE pathman_user1;
SELECT set_auto('permissions.pathman_user1_table', false);

/* Partition creation, should fail */
SET ROLE pathman_user2;
INSERT INTO permissions.pathman_user1_table (id, a) VALUES (55, 0) RETURNING *;

/* Finally drop partitions */
SET ROLE pathman_user1;
SELECT drop_partitions('permissions.pathman_user1_table');


/* Switch to #2 */
SET ROLE pathman_user2;
/* Test ddl event trigger */
CREATE TABLE permissions.pathman_user2_table(id serial);
SELECT create_hash_partitions('permissions.pathman_user2_table', 'id', 3);
INSERT INTO permissions.pathman_user2_table SELECT generate_series(1, 30);
SELECT drop_partitions('permissions.pathman_user2_table');


/* Switch to #1 */
SET ROLE pathman_user1;
CREATE TABLE permissions.dropped_column(a int, val int not null, b int, c int);
INSERT INTO permissions.dropped_column SELECT i,i,i,i FROM generate_series(1, 30) i;

GRANT SELECT(val), INSERT(val) ON permissions.dropped_column TO pathman_user2;

SELECT create_range_partitions('permissions.dropped_column', 'val', 1, 10);

SELECT attrelid::regclass, attname, attacl FROM pg_attribute
WHERE attrelid = ANY (SELECT "partition" FROM pathman_partition_list
					  WHERE parent = 'permissions.dropped_column'::REGCLASS)
	  AND attacl IS NOT NULL
ORDER BY attrelid::regclass::text; /* check ACL for each column */

ALTER TABLE permissions.dropped_column DROP COLUMN a; /* DROP "a" */
SELECT append_range_partition('permissions.dropped_column');

SELECT attrelid::regclass, attname, attacl FROM pg_attribute
WHERE attrelid = ANY (SELECT "partition" FROM pathman_partition_list
					  WHERE parent = 'permissions.dropped_column'::REGCLASS)
	  AND attacl IS NOT NULL
ORDER BY attrelid::regclass::text; /* check ACL for each column (+1 partition) */

ALTER TABLE permissions.dropped_column DROP COLUMN b; /* DROP "b" */
SELECT append_range_partition('permissions.dropped_column');

SELECT attrelid::regclass, attname, attacl FROM pg_attribute
WHERE attrelid = ANY (SELECT "partition" FROM pathman_partition_list
					  WHERE parent = 'permissions.dropped_column'::REGCLASS)
	  AND attacl IS NOT NULL
ORDER BY attrelid::regclass::text; /* check ACL for each column (+1 partition) */

DROP TABLE permissions.dropped_column CASCADE;


/* Finally reset user */
RESET ROLE;

DROP OWNED BY pathman_user1;
DROP OWNED BY pathman_user2;
DROP USER pathman_user1;
DROP USER pathman_user2;


DROP SCHEMA permissions;
DROP EXTENSION pg_pathman;
