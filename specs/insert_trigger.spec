setup
{
	CREATE EXTENSION pg_pathman;
	CREATE TABLE range_rel(id serial primary key);
	SELECT create_range_partitions('range_rel', 'id', 1, 100, 1);
}

teardown
{
	SELECT drop_range_partitions('range_rel');
	DROP TABLE range_rel CASCADE;
	DROP EXTENSION pg_pathman;
}

session "s1"
step "s1b" { BEGIN; }
step "s1_insert_150" { INSERT INTO range_rel SELECT generate_series(1, 150); }
step "s1_insert_300" { INSERT INTO range_rel SELECT generate_series(151, 300); }
step "s1_show_partitions" { SELECT c.consrc FROM pg_inherits i LEFT JOIN pg_constraint c ON c.conrelid = i.inhrelid AND c.consrc IS NOT NULL WHERE i.inhparent = 'range_rel'::regclass::oid ORDER BY c.consrc; }
step "s1r" { ROLLBACK; }
step "s1c" { COMMIT; }

session "s2"
step "s2b" { BEGIN; }
step "s2_insert_150" { INSERT INTO range_rel SELECT generate_series(1, 150); }
step "s2_insert_300" { INSERT INTO range_rel SELECT generate_series(151, 300); }
step "s2_show_partitions" { SELECT c.consrc FROM pg_inherits i LEFT JOIN pg_constraint c ON c.conrelid = i.inhrelid  AND c.consrc IS NOT NULL WHERE i.inhparent = 'range_rel'::regclass::oid ORDER BY c.consrc; }
step "s2r" { ROLLBACK; }
step "s2c" { COMMIT; }

# Rollback first transactions
permutation "s1b" "s1_insert_150" "s1r" "s1_show_partitions" "s2b" "s2_insert_150" "s2c" "s2_show_partitions"

permutation "s1b" "s1_insert_150" "s1r" "s1_show_partitions" "s2b" "s2_insert_300" "s2c" "s2_show_partitions"

permutation "s1b" "s1_insert_300" "s1r" "s1_show_partitions" "s2b" "s2_insert_150" "s2c" "s2_show_partitions"

# Rollback both transactions
permutation "s1b" "s1_insert_150" "s2b" "s2_insert_300" "s1r" "s2r" "s2_show_partitions"
