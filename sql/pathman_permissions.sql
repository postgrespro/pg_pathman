\SET VERBOSITY terse

CREATE EXTENSION pg_pathman;
CREATE ROLE user1 LOGIN;
CREATE ROLE user2 LOGIN;

\SET ROLE user1

CREATE TABLE user1_table(id serial, a int);
SELECT create_hash_partitioning('user1_table', 'id', 3);

ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT INSERT ON TABLES TO public;

\SET ROLE user2

/* Trying to change partitioning parameters for user1_table */
SELECT set_enable_parent('user1_table', true);
