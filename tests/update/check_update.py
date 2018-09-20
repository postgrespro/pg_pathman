#!/usr/bin/env python
#coding: utf-8

import shutil
import os
import contextlib
import sys
import argparse
import testgres
import subprocess
import difflib

my_dir = os.path.dirname(os.path.abspath(__file__))
repo_dir = os.path.abspath(os.path.join(my_dir, '../../'))
print(repo_dir)

compilation = '''
make USE_PGXS=1 clean
make USE_PGXS=1 install
'''

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

@contextlib.contextmanager
def cwd(path):
    print("cwd: ", path)
    curdir = os.getcwd()
    os.chdir(path)

    try:
        yield
    finally:
        print("cwd:", curdir)
        os.chdir(curdir)

def shell(cmd):
    print(cmd)
    subprocess.check_output(cmd, shell=True)

dump1_file = '/tmp/dump1.sql'
dump2_file = '/tmp/dump2.sql'

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='pg_pathman update checker')
    parser.add_argument('branches', nargs=2,
                        help='specify branches ("main rel_1.5")')

    args = parser.parse_args()

    with open(os.path.join(my_dir, 'dump_pathman_objects.sql'), 'r') as f:
        dump_sql = f.read()

    shutil.rmtree('/tmp/pg_pathman')
    shutil.copytree(repo_dir, '/tmp/pg_pathman')

    with cwd('/tmp/pg_pathman'):
        shell("git clean -fdx")
        shell("git reset --hard")
        shell("git checkout %s" % args.branches[0])
        shell(compilation)

        with testgres.get_new_node('updated') as node:
            node.init()
            node.append_conf("shared_preload_libraries='pg_pathman'\n")

            node.start()
            node.safe_psql('postgres', run_sql)
            node.dump(dump1_file, 'postgres')
            node.stop()

            shell("git clean -fdx")
            shell("git checkout %s" % args.branches[1])
            shell(compilation)

            version = None
            with open('pg_pathman.control') as f:
                for line in f.readlines():
                    if line.startswith('default_version'):
                        version = line.split('=')[1].strip()

            if version is None:
                print("cound not find version in second branch")
                exit(1)

            node.start()
            p = subprocess.Popen(["psql", "postgres"], stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE)
            dumped_objects_old = p.communicate(input=dump_sql.encode())[0].decode()
            node.stop()

        # now make clean install
        with testgres.get_new_node('from_scratch') as node:
            node.init()
            node.append_conf("shared_preload_libraries='pg_pathman'\n")
            node.start()
            node.safe_psql('postgres', run_sql)
            p = subprocess.Popen(["psql", "postgres"], stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE)
            dumped_objects_new = p.communicate(input=dump_sql.encode())[0].decode()
            node.dump(dump2_file, 'postgres')

            # check dumps
            node.safe_psql('postgres', 'create database d1')
            node.restore(dump1_file, 'd1')

            node.safe_psql('postgres', 'create database d2')
            node.restore(dump2_file, 'd2')
            node.stop()

    if dumped_objects_old != dumped_objects_new:
        print("\nDIFF:")
        for line in difflib.context_diff(dumped_objects_old.split('\n'), dumped_objects_new.split('\n')):
            print(line)
    else:
        print("\nUPDATE CHECK: ALL GOOD")
