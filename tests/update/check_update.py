#!/usr/bin/env python
#coding: utf-8

import shutil
import os
import contextlib
import sys
import argparse
import testgres
import subprocess
import time

my_dir = os.path.dirname(os.path.abspath(__file__))
repo_dir = os.path.abspath(os.path.join(my_dir, '../../'))
print(repo_dir)

# just bunch of tables to create
run_sql = '''
CREATE EXTENSION pg_pathman;

CREATE TABLE hash_rel (
	id		SERIAL PRIMARY KEY,
	value	INTEGER NOT NULL);
INSERT INTO hash_rel VALUES (1, 1);
INSERT INTO hash_rel VALUES (2, 2);
INSERT INTO hash_rel VALUES (3, 3);

SELECT create_hash_partitions('hash_rel', 'Value', 3);

CREATE TABLE range_rel (
	id	SERIAL PRIMARY KEY,
	dt	TIMESTAMP not null,
	txt	TEXT);
CREATE INDEX ON range_rel (dt);
INSERT INTO range_rel (dt, txt)
SELECT g, md5(g::TEXT) FROM generate_series('2015-01-01', '2015-04-30', '1 day'::interval) as g;
SELECT create_range_partitions('range_rel', 'DT', '2015-01-01'::DATE, '1 month'::INTERVAL);

CREATE TABLE num_range_rel (
	id	SERIAL PRIMARY KEY,
	txt	TEXT);
SELECT create_range_partitions('num_range_rel', 'id', 0, 1000, 4);
INSERT INTO num_range_rel
	SELECT g, md5(g::TEXT) FROM generate_series(1, 3000) as g;

CREATE TABLE improved_dummy_test1 (id BIGSERIAL, name TEXT NOT NULL);
INSERT INTO improved_dummy_test1 (name) SELECT md5(g::TEXT) FROM generate_series(1, 100) as g;
SELECT create_range_partitions('improved_dummy_test1', 'id', 1, 10);
INSERT INTO improved_dummy_test1 (name) VALUES ('test'); /* spawns new partition */
ALTER TABLE improved_dummy_test1 ADD CHECK (name != 'ib');

CREATE TABLE test_improved_dummy_test2 (val INT NOT NULL);
SELECT create_range_partitions('test_improved_dummy_test2', 'val',
									   generate_range_bounds(1, 1, 2),
									   partition_names := '{p1, p2}');

CREATE TABLE insert_into_select(val int NOT NULL);
INSERT INTO insert_into_select SELECT generate_series(1, 100);
SELECT create_range_partitions('insert_into_select', 'val', 1, 20);
CREATE TABLE insert_into_select_copy (LIKE insert_into_select); /* INSERT INTO ... SELECT ... */

-- just a lot of actions

SELECT split_range_partition('num_range_rel_1', 500);
SELECT split_range_partition('range_rel_1', '2015-01-15'::DATE);

/* Merge two partitions into one */
SELECT merge_range_partitions('num_range_rel_1', 'num_range_rel_' || currval('num_range_rel_seq'));
SELECT merge_range_partitions('range_rel_1', 'range_rel_' || currval('range_rel_seq'));

/* Append and prepend partitions */
SELECT append_range_partition('num_range_rel');
SELECT prepend_range_partition('num_range_rel');
SELECT drop_range_partition('num_range_rel_7');

SELECT drop_range_partition_expand_next('num_range_rel_4');
SELECT drop_range_partition_expand_next('num_range_rel_6');

SELECT append_range_partition('range_rel');
SELECT prepend_range_partition('range_rel');
SELECT drop_range_partition('range_rel_7');
SELECT add_range_partition('range_rel', '2014-12-01'::DATE, '2015-01-01'::DATE);

CREATE TABLE range_rel_minus_infinity (LIKE range_rel INCLUDING ALL);
SELECT attach_range_partition('range_rel', 'range_rel_minus_infinity', NULL, '2014-12-01'::DATE);
INSERT INTO range_rel (dt) VALUES ('2012-06-15');
INSERT INTO range_rel (dt) VALUES ('2015-12-15');

CREATE TABLE hash_rel_extern (LIKE hash_rel INCLUDING ALL);
SELECT replace_hash_partition('hash_rel_0', 'hash_rel_extern');

-- automatic partitions creation
CREATE TABLE range_rel_test1 (
	id	SERIAL PRIMARY KEY,
	dt	TIMESTAMP NOT NULL,
	data TEXT);
SELECT create_range_partitions('range_rel_test1', 'dt', '2015-01-01'::DATE, '10 days'::INTERVAL, 1);
INSERT INTO range_rel_test1 (dt)
SELECT generate_series('2015-01-01', '2015-04-30', '1 day'::interval);

INSERT INTO range_rel_test1 (dt)
SELECT generate_series('2014-12-31', '2014-12-01', '-1 day'::interval);

/* CaMeL cAsE table names and attributes */
CREATE TABLE "TeSt" (a INT NOT NULL, b INT);
SELECT create_hash_partitions('"TeSt"', 'a', 3);
INSERT INTO "TeSt" VALUES (1, 1);
INSERT INTO "TeSt" VALUES (2, 2);
INSERT INTO "TeSt" VALUES (3, 3);

CREATE TABLE "RangeRel" (
	id	SERIAL PRIMARY KEY,
	dt	TIMESTAMP NOT NULL,
	txt	TEXT);
INSERT INTO "RangeRel" (dt, txt)
SELECT g, md5(g::TEXT) FROM generate_series('2015-01-01', '2015-01-03', '1 day'::interval) as g;
SELECT create_range_partitions('"RangeRel"', 'dt', '2015-01-01'::DATE, '1 day'::INTERVAL);
SELECT append_range_partition('"RangeRel"');
SELECT prepend_range_partition('"RangeRel"');
SELECT merge_range_partitions('"RangeRel_1"', '"RangeRel_' || currval('"RangeRel_seq"') || '"');
SELECT split_range_partition('"RangeRel_1"', '2015-01-01'::DATE);

CREATE TABLE hash_rel_next1 (
	id		SERIAL PRIMARY KEY,
	value	INTEGER NOT NULL);
INSERT INTO hash_rel_next1 (value) SELECT g FROM generate_series(1, 10000) as g;
SELECT create_hash_partitions('hash_rel_next1', 'value', 3);
'''

def shell(cmd):
    print(cmd)
    cp = subprocess.run(cmd, shell=True)
    if cp.returncode != 0:
        raise subprocess.CalledProcessError(cp.returncode, cmd)
    # print(subprocess.check_output(cmd, shell=True).decode("utf-8"))

def shell_call(cmd):
    print(cmd)
    return subprocess.run(cmd, shell=True)

def reinstall_pathman(tmp_pathman_path, revision):
        if revision == 'worktree':
            shutil.rmtree(tmp_pathman_path)
            shutil.copytree(repo_dir, tmp_pathman_path)
            os.chdir(tmp_pathman_path)
        else:
            os.chdir(tmp_pathman_path)
            shell("git clean -fdx")
            shell("git reset --hard")
            shell("git checkout %s" % revision)
        shell('make USE_PGXS=1 clean && make USE_PGXS=1 install -j4')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='''
    pg_pathman update checker. Testgres is used. Junks into /tmp/pathman_check_update.
    First do some partitioned stuff on new version. Save full database dump to
    dump_new.sql and pathman object definitions to pathman_objects_new.sql.
    Then run old version, do the same stuff. Upgrade and make dumps. Ensure
    dumps are the same. Finally, run regressions tests on upgraded version.
    ''')
    parser.add_argument('branches', nargs=2,
                        help='specify branches <old> <new>, e.g. "d34a77e master". Special value "worktree" means, well, working tree.')
    args = parser.parse_args()
    old_branch, new_branch = args.branches[0], args.branches[1]

    pathman_objs_script = os.path.join(my_dir, 'dump_pathman_objects.sql')

    data_prefix = "/tmp/pathman_check_update"
    if os.path.isdir(data_prefix):
        shutil.rmtree(data_prefix)
    dump_new_path = os.path.join(data_prefix, 'dump_new.sql')
    dump_updated_path = os.path.join(data_prefix, 'dump_updated.sql')
    dump_diff_path = os.path.join(data_prefix, 'dump.diff')
    pathman_objs_new_path = os.path.join(data_prefix, 'pathman_objects_new.sql')
    pathman_objs_updated_path = os.path.join(data_prefix, 'pathman_objects_updated.sql')
    pathman_objs_diff_path = os.path.join(data_prefix, 'pathman_objs.diff')
    tmp_pathman_path = os.path.join(data_prefix, "pg_pathman")

    shutil.copytree(repo_dir, tmp_pathman_path)

    reinstall_pathman(tmp_pathman_path, new_branch)
    with testgres.get_new_node('brand_new') as node:
        node.init()
        node.append_conf("shared_preload_libraries='pg_pathman'\n")
        node.start()
        node.safe_psql('postgres', run_sql)
        node.dump(dump_new_path, 'postgres')
        # default user is current OS one
        shell("psql -p {} -h {} -f {} -X -q -a -At > {} 2>&1".format(node.port, node.host, pathman_objs_script, pathman_objs_new_path))
        node.stop()

    # now install old version...
    reinstall_pathman(tmp_pathman_path, old_branch)
    with testgres.get_new_node('updated') as node:
        node.init()
        node.append_conf("shared_preload_libraries='pg_pathman'\n")

        node.start()
        # do the same stuff...
        node.safe_psql('postgres', run_sql)
        # and prepare regression db, see below
        node.safe_psql('postgres', 'create database contrib_regression')
        node.safe_psql('contrib_regression', 'create extension pg_pathman')

        # and upgrade pathman
        node.stop()
        reinstall_pathman(tmp_pathman_path, new_branch)
        node.start()
        print("Running updated db on port {}, datadir {}".format(node.port, node.base_dir))
        node.safe_psql('postgres', "alter extension pg_pathman update")
        node.safe_psql('postgres', "set pg_pathman.enable = t;")

        # regression tests db, see below
        node.safe_psql('contrib_regression', "alter extension pg_pathman update")
        node.safe_psql('contrib_regression', "set pg_pathman.enable = t;")

        node.dump(dump_updated_path, 'postgres')
        # time.sleep(432432)
        # default user is current OS one
        shell("psql -p {} -h {} -f {} -X -q -a -At > {} 2>&1".format(node.port, node.host, pathman_objs_script, pathman_objs_updated_path))

        # check diffs
        shell_call("diff -U3 {} {} > {} 2>&1".format(dump_updated_path, dump_new_path, dump_diff_path))
        if os.stat(dump_diff_path).st_size != 0:
            msg = "DB dumps are not equal, check out the diff at {}\nProbably that's actually ok, please eyeball the diff manually and say, continue?".format(dump_diff_path)
            if input("%s (y/N) " % msg).lower() != 'y':
                sys.exit(1)
        shell_call("diff -U3 {} {} > {} 2>&1".format(pathman_objs_updated_path, pathman_objs_new_path, pathman_objs_diff_path))
        if os.stat(pathman_objs_diff_path).st_size != 0:
            print("pathman objects dumps are not equal, check out the diff at {}".format(pathman_objs_diff_path))
            # sys.exit(1)

        print("just in case, checking that dump can be restored...")
        node.safe_psql('postgres', 'create database tmp')
        node.restore(dump_updated_path, 'tmp')

        print("finally, run (some) pathman regression tests")
        # This is a bit tricky because we want to run tests on exactly this
        # installation of extension. It means we must create db beforehand,
        # tell pg_regress not create it and discard all create/drop extension
        # from tests.
        # Not all tests can be thus adapted instantly, so I think that's enough
        # for now.
        # generated with smth like  ls ~/postgres/pg_pathman/sql/ | sort | sed 's/.sql//' | xargs -n 1 printf "'%s',\n"
        os.chdir(tmp_pathman_path)
        REGRESS = ['pathman_array_qual',
                   'pathman_bgw',
                   'pathman_callbacks',
                   'pathman_column_type',
                   'pathman_cte',
                   'pathman_domains',
                   'pathman_dropped_cols',
                   'pathman_expressions',
                   'pathman_foreign_keys',
                   'pathman_gaps',
                   'pathman_inserts',
                   'pathman_interval',
                   'pathman_lateral',
                   'pathman_only',
                   'pathman_param_upd_del',
                   'pathman_permissions',
                   'pathman_rebuild_deletes',
                   'pathman_rebuild_updates',
                   'pathman_rowmarks',
                   'pathman_subpartitions',
                   'pathman_update_node',
                   'pathman_update_triggers',
                   'pathman_utility_stmt',
                   'pathman_views'
        ]
        outfiles = os.listdir(os.path.join(tmp_pathman_path, 'expected'))
        for tname in REGRESS:
            shell("sed -i '/CREATE EXTENSION pg_pathman;/d' sql/{}.sql".format(tname))
            # CASCADE also removed
            shell("sed -i '/DROP EXTENSION pg_pathman/d' sql/{}.sql".format(tname))
            # there might be more then one .out file
            for outfile in outfiles:
                if outfile.startswith(tname):
                    shell("sed -i '/CREATE EXTENSION pg_pathman;/d' expected/{}".format(outfile))
                    shell("sed -i '/DROP EXTENSION pg_pathman/d' expected/{}".format(outfile))

        # time.sleep(43243242)
        shell("make USE_PGXS=1 PGPORT={} EXTRA_REGRESS_OPTS=--use-existing REGRESS='{}' installcheck 2>&1".format(node.port, " ".join(REGRESS)))

        node.stop()

        print("It's Twelve O'clock and All's Well.")
