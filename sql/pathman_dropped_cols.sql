\set VERBOSITY terse

SET search_path = 'public';
CREATE EXTENSION pg_pathman;
CREATE SCHEMA dropped_cols;


/*
 * we should be able to manage tables with dropped columns
 */

create table test_range(a int, b int, key int not null);

alter table test_range drop column a;
select create_range_partitions('test_range', 'key', 1, 10, 2);

alter table test_range drop column b;
select prepend_range_partition('test_range');

select * from pathman_partition_list order by parent, partition;
select pg_get_constraintdef(oid, true) from pg_constraint where conname = 'pathman_test_range_1_check';
select pg_get_constraintdef(oid, true) from pg_constraint where conname = 'pathman_test_range_3_check';

drop table test_range cascade;


create table test_hash(a int, b int, key int not null);

alter table test_hash drop column a;
select create_hash_partitions('test_hash', 'key', 3);

alter table test_hash drop column b;
create table test_dummy (like test_hash);
select replace_hash_partition('test_hash_2', 'test_dummy', true);

select * from pathman_partition_list order by parent, partition;
select pg_get_constraintdef(oid, true) from pg_constraint where conname = 'pathman_test_hash_1_check';
select pg_get_constraintdef(oid, true) from pg_constraint where conname = 'pathman_test_dummy_check';
drop table test_hash cascade;

-- Yury Smirnov case
CREATE TABLE root_dict (
  id         BIGSERIAL PRIMARY KEY NOT NULL,
  root_id    BIGINT                NOT NULL,
  start_date DATE,
  num        TEXT,
  main       TEXT,
  dict_code  TEXT,
  dict_name  TEXT,
  edit_num   TEXT,
  edit_date  DATE,
  sign       CHAR(4)
);

CREATE INDEX "root_dict_root_id_idx" ON "root_dict" ("root_id");

DO
$$
DECLARE
  r RECORD;
BEGIN
  FOR r IN SELECT * FROM generate_series(1, 3) r
  LOOP
    FOR d IN 1..2 LOOP
      INSERT INTO root_dict (root_id, start_date, num, main, dict_code, dict_name, edit_num, edit_date, sign) VALUES
        (r.r, '2010-10-10'::date, 'num_' || d, (d % 2) + 1, 'code_' || d, 'name_' || d, NULL, NULL, '2014');
    END LOOP;
  END LOOP;
END
$$;

ALTER TABLE root_dict ADD COLUMN dict_id BIGINT DEFAULT 3;
ALTER TABLE root_dict DROP COLUMN dict_code,
					  DROP COLUMN dict_name,
					  DROP COLUMN sign;

SELECT create_hash_partitions('root_dict' :: REGCLASS,
                              'root_id',
                              3,
                              true);
VACUUM FULL ANALYZE "root_dict";
SELECT set_enable_parent('root_dict' :: REGCLASS, FALSE);

PREPARE getbyroot AS
SELECT
	id, root_id, start_date, num, main, edit_num, edit_date, dict_id
FROM root_dict
WHERE root_id = $1;

EXECUTE getbyroot(2);
EXECUTE getbyroot(2);
EXECUTE getbyroot(2);
EXECUTE getbyroot(2);
EXECUTE getbyroot(2);

-- errors usually start here
EXECUTE getbyroot(2);
EXECUTE getbyroot(2);
EXPLAIN (COSTS OFF) EXECUTE getbyroot(2);

DEALLOCATE getbyroot;
DROP TABLE root_dict CASCADE;
DROP SCHEMA dropped_cols;
DROP EXTENSION pg_pathman;
