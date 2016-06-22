setup
{
	create extension pg_pathman;
	create table test_tbl(id int not null, val real);
	insert into test_tbl select i, i from generate_series(1, 1000) as i;
	select create_range_partitions('test_tbl', 'id', 1, 100, 10);
}

teardown
{
	drop table test_tbl cascade;
	drop extension pg_pathman;
}

session "s1"
step "s1_b" { begin; }
step "s1_c" { commit; }
step "s1_r" { rollback; }
step "s1_update" { update test_tbl set id = 2 where id = 1; }

session "s2"
step "s2_b" { begin; }
step "s2_c" { commit; }
step "s2_select_locked" { select * from test_tbl where id = 1 for share; }
step "s2_select" { select * from test_tbl where id = 1; }


permutation "s1_b" "s1_update" "s2_select" "s1_r"

permutation "s1_b" "s1_update" "s2_select_locked" "s1_r"

permutation "s1_b" "s1_update" "s2_select_locked" "s1_c"
