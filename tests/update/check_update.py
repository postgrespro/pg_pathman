#!/usr/bin/env python
#coding: utf-8

import os
import contextlib
import sys
import argparse
import testgres
import subprocess
import difflib

repo_dir = os.path.abspath(os.path.join('../..', os.path.dirname(__file__)))

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
	dt	TIMESTAMP,
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
ALTER TABLE improved_dummy_1 ADD CHECK (name != 'ib'); /* make improved_dummy_1 disappear */

CREATE TABLE test_improved_dummy_test2 (val INT NOT NULL);
SELECT create_range_partitions('test_improved_dummy_test2', 'val',
									   generate_range_bounds(1, 1, 2),
									   partition_names := '{p1, p2}');

CREATE TABLE insert_into_select(val int NOT NULL);
INSERT INTO insert_into_select SELECT generate_series(1, 100);
SELECT create_range_partitions('insert_into_select', 'val', 1, 20);
CREATE TABLE insert_into_select_copy (LIKE insert_into_select); /* INSERT INTO ... SELECT ... */

# just a lot of actions

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
SELECT add_range_partition('range_rel', '2014-12-01'::DATE, '2015-01-02'::DATE);
SELECT add_range_partition('range_rel', '2014-12-01'::DATE, '2015-01-01'::DATE);

CREATE TABLE range_rel_archive (LIKE range_rel INCLUDING ALL);
SELECT attach_range_partition('range_rel', 'range_rel_archive', '2014-01-01'::DATE, '2015-01-01'::DATE);
SELECT attach_range_partition('range_rel', 'range_rel_archive', '2014-01-01'::DATE, '2014-12-01'::DATE);
SELECT detach_range_partition('range_rel_archive');

CREATE TABLE range_rel_test1 (
	id  SERIAL PRIMARY KEY,
	dt  TIMESTAMP,
	txt TEXT,
	abc INTEGER);
SELECT attach_range_partition('range_rel', 'range_rel_test1', '2013-01-01'::DATE, '2014-01-01'::DATE);
CREATE TABLE range_rel_test2 (
	id  SERIAL PRIMARY KEY,
	dt  TIMESTAMP);
SELECT attach_range_partition('range_rel', 'range_rel_test2', '2013-01-01'::DATE, '2014-01-01'::DATE);

/* Half open ranges */
SELECT add_range_partition('range_rel', NULL, '2014-12-01'::DATE, 'range_rel_minus_infinity');
SELECT add_range_partition('range_rel', '2015-06-01'::DATE, NULL, 'range_rel_plus_infinity');
SELECT append_range_partition('range_rel');
SELECT prepend_range_partition('range_rel');

CREATE TABLE range_rel_minus_infinity (LIKE range_rel INCLUDING ALL);
SELECT attach_range_partition('range_rel', 'range_rel_minus_infinity', NULL, '2014-12-01'::DATE);
INSERT INTO range_rel (dt) VALUES ('2012-06-15');
INSERT INTO range_rel (dt) VALUES ('2015-12-15');

CREATE TABLE zero(
	id		SERIAL PRIMARY KEY,
	value	INT NOT NULL);
INSERT INTO zero SELECT g, g FROM generate_series(1, 100) as g;
SELECT create_range_partitions('zero', 'value', 50, 10, 0);
SELECT append_range_partition('zero', 'zero_0');
SELECT prepend_range_partition('zero', 'zero_1');
SELECT add_range_partition('zero', 50, 70, 'zero_50');
SELECT append_range_partition('zero', 'zero_appended');
SELECT prepend_range_partition('zero', 'zero_prepended');
SELECT split_range_partition('zero_50', 60, 'zero_60');

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
SELECT create_hash_partitions('TeSt', 'a', 3);
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

CREATE TABLE range_rel_next1 (
	id		SERIAL PRIMARY KEY,
	dt		TIMESTAMP NOT NULL,
	value	INTEGER);
INSERT INTO range_rel_next1 (dt, value) SELECT g, extract(day from g) FROM generate_series('2010-01-01'::date, '2010-12-31'::date, '1 day') as g;
SELECT create_range_partitions('range_rel_next1', 'dt', '2010-01-01'::date, '1 month'::interval, 12);
SELECT merge_range_partitions('range_rel_1', 'range_rel_2');
SELECT split_range_partition('range_rel_1', '2010-02-15'::date);
SELECT append_range_partition('range_rel_next1');
SELECT prepend_range_partition('range_rel_next1');
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

dump1_file = '/tmp/dump1.sql'
dump2_file = '/tmp/dump2.sql'

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='pg_pathman update checker')
    parser.add_argument('branches', nargs=2,
                        help='specify branches ("main rel_1.5")')

    args = parser.parse_args()

    with open('dump_pathman_objects.sql') as f:
        dump_sql = f.read()

    with cwd(repo_dir):
        subprocess.check_output("git checkout %s" % args.branches[0], shell=True)
        subprocess.check_output(compilation, shell=True)

        with testgres.get_new_node('updated') as node:
            node.init()
            node.append_conf("shared_preload_libraries='pg_pathman'\n")

            node.start()
            node.safe_psql('postgres', run_sql)
            node.dump(dump1_file, 'postgres')
            node.stop()

            subprocess.check_output("git checkout %s" % args.branches[1], shell=True)
            subprocess.check_output(compilation, shell=True)

            version = None
            with open('pg_pathman.control') as f:
                for line in f.readlines():
                    if line.startswith('default_version'):
                        version = line.split('=').strip()

            if version is None:
                print("cound not find version in second branch")
                exit(1)

            node.start()
            node.safe_psql("postgres", "alter extension pg_pathman update to %s" % version)
            dumped_objects_old = node.safe_psql("postgres", dump_sql)
            node.stop()

        # now make clean install
        with testgres.get_new_node('from_scratch') as node:
            node.init()
            node.append_conf("shared_preload_libraries='pg_pathman'\n")
            node.start()
            node.safe_psql('postgres', run_sql)
            dumped_objects_new = node.safe_psql("postgres", dump_sql)
            node.dump(dump2_file, 'postgres')

            # check dumps
            node.safe_psql('postgres', 'create database d1')
            node.restore(dump1_file, 'd1')

            node.safe_psql('postgres', 'create database d2')
            node.restore(dump2_file, 'd2')
            node.stop()

    if dumped_objects != dumped_objects_new:
        pass
