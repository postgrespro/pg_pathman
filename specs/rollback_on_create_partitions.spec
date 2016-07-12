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
step "show_me_what_you_got" { EXPLAIN (COSTS OFF) SELECT * FROM range_rel; }

permutation "begin" "insert_data" "create_partitions" "rollback" "show_me_what_you_got"
permutation "begin" "insert_data" "create_partitions" "commit" "show_me_what_you_got"
