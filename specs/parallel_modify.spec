# Test for parallel modification of rows in a partitioned table.

setup
{
  CREATE EXTENSION pg_pathman;

  /* Create simple table and insert rows. */
  CREATE SCHEMA test_schema;
  CREATE TABLE test_schema.t (id bigint PRIMARY KEY, dt date NOT NULL, t1 text, t2 text, t3 text);
  INSERT INTO test_schema.t VALUES (1, DATE '2026-01-30', 't1', 't2', 't3');

  /* Define partitioned table. */
  SELECT create_range_partitions('test_schema.t'::regclass, 'dt', '2026-01-01'::DATE, '1 month'::INTERVAL, 6, true);
  SELECT set_enable_parent('test_schema.t'::regclass, false);

  /* Create triggers (for each partition). */
  CREATE FUNCTION test_schema.t_before_update() RETURNS trigger
  LANGUAGE plpgsql AS $$
  BEGIN
    NEW.t3 := 't3_new';
    RETURN NEW;
  END;
  $$;

  DO $$
  DECLARE
    part regclass;
  BEGIN
    FOR part IN SELECT partition FROM pathman_partition_list WHERE parent = 'test_schema.t'::regclass
    LOOP
      EXECUTE format('CREATE TRIGGER t_before_update BEFORE UPDATE ON %s '
                     'FOR EACH ROW EXECUTE FUNCTION test_schema.t_before_update()', part);
    END LOOP;
  END;
  $$;
}

teardown
{
  DROP SCHEMA test_schema CASCADE;
  DROP EXTENSION pg_pathman;
}

session s1
step s1		{ BEGIN; }
step s1epr	{ SET pg_pathman.enable_partitionrouter = ON; }
step s1u	{ UPDATE test_schema.t SET dt = dt + '1 day'::INTERVAL WHERE id = 1; }
step s1u2	{ UPDATE test_schema.t SET dt = dt + '2 day'::INTERVAL WHERE id = 1; }
step s1c	{ COMMIT; }
step s1r	{ ROLLBACK; }
step s1s	{ SELECT tableoid::regclass, * FROM test_schema.t ORDER BY tableoid::regclass::text COLLATE "C", id; }

session s2
step s2		{ BEGIN; }
step s2epr	{ SET pg_pathman.enable_partitionrouter = ON; }
step s2u	{ UPDATE test_schema.t SET dt  = dt + '1 day'::INTERVAL, t1 = 't1_new', t2 = 't2_new' WHERE id = 1; }
step s2u2	{ UPDATE test_schema.t SET dt  = dt - '2 day'::INTERVAL, t1 = 't1_new', t2 = 't2_new' WHERE id = 1; }
step s2u3	{ UPDATE test_schema.t SET dt  = dt + '2 day'::INTERVAL, t1 = 't1_new', t2 = 't2_new' WHERE id = 1; }
step s2c	{ COMMIT; }
step s2r	{ ROLLBACK; }

# Session 1 strats modifying a row (without partition routing).
# Then session 2 strats modifying the same row and waits.
# Session 1 commits the changes, session 2 gets changes of session 1 with using EPQ.
# Finally session 2 changes the row with using partition routing.
permutation s1s s1 s1u s2 s2epr s2u(s1c) s1c(*) s2c s1s

# Session 1 strats modifying a row (with partition routing).
# Then session 2 strats modifying the same row and waits.
# Session 1 commits the changes, session 2 gets changes of session 1 with using EPQ.
# Finally session 2 tries to changes the row without using partition routing and gets error
# "tuple to be locked was already moved to another partition due to concurrent update".
permutation s1s s1epr s1 s1u2 s2 s2u(s1c) s1c(*) s2c s1s

# Session 1 strats modifying a row (without partition routing).
# Then session 2 strats modifying the same row and waits.
# Session 1 commits the changes, session 2 gets changes of session 1 with using EPQ.
# Finally session 2 changes the row without partition routing.
permutation s1s s1 s1u s2 s2u2(s1c) s1c(*) s2c s1s

# Session 1 strats modifying a row (with partition routing).
# Then session 2 strats modifying the same row and waits.
# Session 1 rollbacks the changes, session 2 continues without changes of session 1.
# Finally session 2 changes the row with using partition routing.
permutation s1s s1epr s1 s1u2 s2 s2u3(s1r) s1r(*) s2c s1s

# Session 1 strats modifying a row (without partition routing).
# Then session 2 strats modifying the same row and waits.
# Session 1 commits the changes, session 2 gets changes of session 1 with using EPQ.
# Finally session 2 rollbacks the changes.
permutation s1s s1 s1u s2 s2epr s2u(s1c) s1c(*) s2r s1s
