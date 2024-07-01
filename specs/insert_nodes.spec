setup
{
	CREATE EXTENSION pg_pathman;
	CREATE TABLE range_rel(id serial primary key);
	SELECT create_range_partitions('range_rel', 'id', 1, 100, 1);
	SELECT set_spawn_using_bgw('range_rel', true);
}

teardown
{
	SELECT drop_partitions('range_rel');
	DROP TABLE range_rel CASCADE;
	DROP EXTENSION pg_pathman;
}

session "s1"
step "s1b"					{ BEGIN; }
step "s1_insert_150"		{ INSERT INTO range_rel SELECT generate_series(1,   150); }
step "s1_insert_300"		{ INSERT INTO range_rel SELECT generate_series(151, 300); }
step "s1_show_partitions"	{ SELECT pg_get_constraintdef(c.oid) FROM pg_inherits i LEFT JOIN pg_constraint c
							  ON c.conrelid = i.inhrelid
							  WHERE i.inhparent = 'range_rel'::regclass AND c.contype = 'c'
							  ORDER BY c.oid; }
step "s1r"					{ ROLLBACK; }

session "s2"
step "s2b"					{ BEGIN; }
step "s2_insert_150"		{ INSERT INTO range_rel SELECT generate_series(1,   150); }
step "s2_insert_300"		{ INSERT INTO range_rel SELECT generate_series(151, 300); }
step "s2_show_partitions"	{ SELECT pg_get_constraintdef(c.oid) FROM pg_inherits i LEFT JOIN pg_constraint c
							  ON c.conrelid = i.inhrelid
							  WHERE i.inhparent = 'range_rel'::regclass AND c.contype = 'c'
							  ORDER BY c.oid; }
step "s2r"					{ ROLLBACK; }
step "s2c"					{ COMMIT; }

# Rollback first transactions
permutation "s1b" "s1_insert_150" "s1r" "s1_show_partitions" "s2b" "s2_insert_150" "s2c" "s2_show_partitions"

permutation "s1b" "s1_insert_150" "s1r" "s1_show_partitions" "s2b" "s2_insert_300" "s2c" "s2_show_partitions"

permutation "s1b" "s1_insert_300" "s1r" "s1_show_partitions" "s2b" "s2_insert_150" "s2c" "s2_show_partitions"

# Rollback both transactions
permutation "s1b" "s1_insert_150" "s2b" "s2_insert_300" "s1r" "s2r" "s2_show_partitions"
