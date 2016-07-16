setup
{
	CREATE EXTENSION pg_pathman;
	CREATE TABLE range_rel(id serial primary key);
}

teardown
{
	SELECT drop_range_partitions('range_rel');
	DROP TABLE range_rel CASCADE;
	DROP EXTENSION pg_pathman;
}

session "s1"
step "begin" { BEGIN; }
step "rollback" { ROLLBACK; }
step "commit" { COMMIT; }
step "insert_data" { INSERT INTO range_rel SELECT generate_series(1, 10000); }
step "create_partitions" { SELECT create_range_partitions('range_rel', 'id', 1, 1000); }
step "drop_partitions" { SELECT drop_range_partitions('range_rel'); }
step "savepoint_a" { SAVEPOINT a; }
step "rollback_a" { ROLLBACK TO SAVEPOINT a; }
step "savepoint_b" { SAVEPOINT b; }
step "rollback_b" { ROLLBACK TO SAVEPOINT b; }
step "savepoint_c" { SAVEPOINT c; }
step "show_rel" { EXPLAIN (COSTS OFF) SELECT * FROM range_rel; }

permutation "begin" "insert_data" "create_partitions" "show_rel" "rollback" "show_rel"

permutation "begin" "insert_data" "create_partitions" "show_rel" "commit" "show_rel"

permutation "begin" "insert_data" "savepoint_a" "create_partitions" "savepoint_b" "drop_partitions" "show_rel" "savepoint_c" "rollback" "show_rel"
permutation "begin" "insert_data" "savepoint_a" "create_partitions" "savepoint_b" "drop_partitions" "show_rel" "savepoint_c" "commit" "show_rel"

# rollback to 'b' after dropping partitions
permutation "begin" "insert_data" "savepoint_a" "create_partitions" "savepoint_b" "drop_partitions" "savepoint_c" "rollback_b" "show_rel" "rollback" "show_rel"
permutation "begin" "insert_data" "savepoint_a" "create_partitions" "savepoint_b" "drop_partitions" "savepoint_c" "rollback_b" "show_rel" "commit" "show_rel"

# rollback to 'a' after dropping partitions
permutation "begin" "insert_data" "savepoint_a" "create_partitions" "savepoint_b" "drop_partitions" "show_rel" "savepoint_c" "rollback_a" "show_rel" "rollback" "show_rel"
permutation "begin" "insert_data" "savepoint_a" "create_partitions" "savepoint_b" "drop_partitions" "show_rel" "savepoint_c" "rollback_a" "show_rel" "commit" "show_rel"

# drop partitions twice in a single transaction
permutation "begin" "insert_data" "savepoint_a" "create_partitions" "savepoint_b" "drop_partitions" "show_rel" "savepoint_c" "rollback_b" "drop_partitions" "show_rel" "rollback" "show_rel"
permutation "begin" "insert_data" "savepoint_a" "create_partitions" "savepoint_b" "drop_partitions" "show_rel" "savepoint_c" "rollback_b" "drop_partitions" "show_rel" "commit" "show_rel"

# create partitions twice in a single transaction
permutation "begin" "insert_data" "savepoint_a" "create_partitions" "savepoint_b" "drop_partitions" "rollback_a" "create_partitions" "show_rel" "rollback" "show_rel"
permutation "begin" "insert_data" "savepoint_a" "create_partitions" "savepoint_b" "drop_partitions" "rollback_a" "create_partitions" "show_rel" "commit" "show_rel"
