# coding: utf-8
"""
 concurrent_partitioning_test.py
		Tests concurrent partitioning worker with simultaneous update queries

 Copyright (c) 2015-2016, Postgres Professional
"""

import unittest
from testgres import get_new_node, stop_all
import time
import os
import threading


def if_fdw_enabled(func):
	"""To run tests with FDW support set environment variable TEST_FDW=1"""
	def wrapper(*args, **kwargs):
		if os.environ.get('FDW_DISABLED') != '1':
			func(*args, **kwargs)
		else:
			print('Warning: FDW features tests are disabled, skipping...')
	return wrapper


class PartitioningTests(unittest.TestCase):

	def setUp(self):
		self.setup_cmd = [
			'create table abc(id serial, t text)',
			'insert into abc select generate_series(1, 300000)',
			'select create_hash_partitions(\'abc\', \'id\', 3, partition_data := false)',
		]

	def tearDown(self):
		stop_all()

	def start_new_pathman_cluster(self, name='test', allows_streaming=False):
		node = get_new_node(name)
		node.init(allows_streaming=allows_streaming)
		node.append_conf(
			'postgresql.conf',
			'shared_preload_libraries=\'pg_pathman\'\n')
		node.start()
		node.psql('postgres', 'create extension pg_pathman')
		return node

	def init_test_data(self, node):
		"""Initialize pg_pathman extension and test data"""
		for cmd in self.setup_cmd:
			node.safe_psql('postgres', cmd)

	def catchup_replica(self, master, replica):
		"""Wait until replica synchronizes with master"""
		master.poll_query_until(
			'postgres',
			'SELECT pg_current_xlog_location() <= replay_location '
			'FROM pg_stat_replication WHERE application_name = \'%s\''
			% replica.name)

	def printlog(self, logfile):
		with open(logfile, 'r') as log:
			for line in log.readlines():
				print(line)

	def test_concurrent(self):
		"""Tests concurrent partitioning"""
		try:
			node = self.start_new_pathman_cluster()
			self.init_test_data(node)

			node.psql(
				'postgres',
				'select partition_table_concurrently(\'abc\')')

			while True:
				# update some rows to check for deadlocks
				node.safe_psql(
					'postgres',
					'''
						update abc set t = 'test'
						where id in (select (random() * 300000)::int
						from generate_series(1, 3000))
					''')

				count = node.execute(
					'postgres',
					'select count(*) from pathman_concurrent_part_tasks')

				# if there is no active workers then it means work is done
				if count[0][0] == 0:
					break
				time.sleep(1)

			data = node.execute('postgres', 'select count(*) from only abc')
			self.assertEqual(data[0][0], 0)
			data = node.execute('postgres', 'select count(*) from abc')
			self.assertEqual(data[0][0], 300000)

			node.stop()
		except Exception, e:
			self.printlog(node.logs_dir + '/postgresql.log')
			raise e

	def test_replication(self):
		"""Tests how pg_pathman works with replication"""
		node = get_new_node('master')
		replica = get_new_node('repl')

		try:
			# initialize master server
			node = self.start_new_pathman_cluster(allows_streaming=True)
			node.backup('my_backup')

			# initialize replica from backup
			replica.init_from_backup(node, 'my_backup', has_streaming=True)
			replica.start()

			# initialize pg_pathman extension and some test data
			self.init_test_data(node)

			# wait until replica catches up
			self.catchup_replica(node, replica)

			# check that results are equal
			self.assertEqual(
				node.psql('postgres', 'explain (costs off) select * from abc'),
				replica.psql('postgres', 'explain (costs off) select * from abc')
			)

			# enable parent and see if it is enabled in replica
			node.psql('postgres', 'select enable_parent(\'abc\'')

			self.catchup_replica(node, replica)
			self.assertEqual(
				node.psql('postgres', 'explain (costs off) select * from abc'),
				replica.psql('postgres', 'explain (costs off) select * from abc')
			)
			self.assertEqual(
				node.psql('postgres', 'select * from abc'),
				replica.psql('postgres', 'select * from abc')
			)
			self.assertEqual(
				node.execute('postgres', 'select count(*) from abc')[0][0],
				300000
			)

			# check that direct UPDATE in pathman_config_params invalidates
			# cache
			node.psql(
				'postgres',
				'update pathman_config_params set enable_parent = false')
			self.catchup_replica(node, replica)
			self.assertEqual(
				node.psql('postgres', 'explain (costs off) select * from abc'),
				replica.psql('postgres', 'explain (costs off) select * from abc')
			)
			self.assertEqual(
				node.psql('postgres', 'select * from abc'),
				replica.psql('postgres', 'select * from abc')
			)
			self.assertEqual(
				node.execute('postgres', 'select count(*) from abc')[0][0],
				0
			)
		except Exception, e:
			self.printlog(node.logs_dir + '/postgresql.log')
			self.printlog(replica.logs_dir + '/postgresql.log')
			raise e

	def test_locks(self):
		"""Test that a session trying to create new partitions waits for other
		sessions if they doing the same"""

		import threading
		import time

		class Flag:
			def __init__(self, value):
				self.flag = value

			def set(self, value):
				self.flag = value

			def get(self):
				return self.flag

		# There is one flag for each thread which shows if thread have done
		# its work
		flags = [Flag(False) for i in xrange(3)]

		# All threads synchronizes though this lock
		lock = threading.Lock()

		# Define thread function
		def add_partition(node, flag, query):
			""" We expect that this query will wait until another session
			commits or rolls back"""
			node.safe_psql('postgres', query)
			with lock:
				flag.set(True)

		# Initialize master server
		node = get_new_node('master')

		try:
			node.init()
			node.append_conf(
				'postgresql.conf',
				'shared_preload_libraries=\'pg_pathman\'\n')
			node.start()
			node.safe_psql(
				'postgres',
				'create extension pg_pathman; ' +
				'create table abc(id serial, t text); ' +
				'insert into abc select generate_series(1, 100000); ' +
				'select create_range_partitions(\'abc\', \'id\', 1, 50000);'
			)

			# Start transaction that will create partition
			con = node.connect()
			con.begin()
			con.execute('select append_range_partition(\'abc\')')

			# Start threads that suppose to add new partitions and wait some
			# time
			query = [
				'select prepend_range_partition(\'abc\')',
				'select append_range_partition(\'abc\')',
				'select add_range_partition(\'abc\', 500000, 550000)',
			]
			threads = []
			for i in range(3):
				thread = threading.Thread(
					target=add_partition,
					args=(node, flags[i], query[i]))
				threads.append(thread)
				thread.start()
			time.sleep(3)

			# This threads should wait until current transaction finished
			with lock:
				for i in range(3):
					self.assertEqual(flags[i].get(), False)

			# Commit transaction. Since then other sessions can create
			# partitions
			con.commit()

			# Now wait until each thread finishes
			for thread in threads:
				thread.join()

			# Check flags, it should be true which means that threads are
			# finished
			with lock:
				for i in range(3):
					self.assertEqual(flags[i].get(), True)

			# Check that all partitions are created
			self.assertEqual(
				node.safe_psql(
					'postgres',
					'select count(*) from pg_inherits where inhparent=\'abc\'::regclass'
				),
				'6\n'
			)
		except Exception, e:
			self.printlog(node.logs_dir + '/postgresql.log')
			raise e

	def test_tablespace(self):
		"""Check tablespace support"""

		def check_tablespace(node, tablename, tablespace):
			res = node.execute(
				'postgres',
				'select get_tablespace(\'{}\')'.format(tablename))
			if len(res) == 0:
				return False

			return res[0][0] == tablespace

		node = get_new_node('master')
		node.init()
		node.append_conf(
			'postgresql.conf',
			'shared_preload_libraries=\'pg_pathman\'\n')
		node.start()
		node.psql('postgres', 'create extension pg_pathman')

		# create tablespace
		path = os.path.join(node.data_dir, 'test_space_location')
		os.mkdir(path)
		node.psql(
			'postgres',
			'create tablespace test_space location \'{}\''.format(path))

		# create table in this tablespace
		node.psql(
			'postgres',
			'create table abc(a serial, b int) tablespace test_space')

		# create three partitions. Excpect that they will be created in the
		# same tablespace as the parent table
		node.psql(
			'postgres',
			'select create_range_partitions(\'abc\', \'a\', 1, 10, 3)')
		self.assertTrue(check_tablespace(node, 'abc', 'test_space'))

		# check tablespace for appended partition
		node.psql(
			'postgres',
			'select append_range_partition(\'abc\', \'abc_appended\')')
		self.assertTrue(check_tablespace(node, 'abc_appended', 'test_space'))

		# check tablespace for prepended partition
		node.psql(
			'postgres',
			'select prepend_range_partition(\'abc\', \'abc_prepended\')')
		self.assertTrue(check_tablespace(node, 'abc_prepended', 'test_space'))

		# check tablespace for prepended partition
		node.psql(
			'postgres',
			'select add_range_partition(\'abc\', 41, 51, \'abc_added\')')
		self.assertTrue(check_tablespace(node, 'abc_added', 'test_space'))

		# check tablespace for split
		node.psql(
			'postgres',
			'select split_range_partition(\'abc_added\', 45, \'abc_splitted\')')
		self.assertTrue(check_tablespace(node, 'abc_splitted', 'test_space'))

		# now let's specify tablespace explicitly
		node.psql(
			'postgres',
			'select append_range_partition(\'abc\', \'abc_appended_2\', \'pg_default\')')
		node.psql(
			'postgres',
			'select prepend_range_partition(\'abc\', \'abc_prepended_2\', \'pg_default\')')
		node.psql(
			'postgres',
			'select add_range_partition(\'abc\', 61, 71, \'abc_added_2\', \'pg_default\')')
		node.psql(
			'postgres',
			'select split_range_partition(\'abc_added_2\', 65, \'abc_splitted_2\', \'pg_default\')')
		self.assertTrue(check_tablespace(node, 'abc_appended_2', 'pg_default'))
		self.assertTrue(check_tablespace(node, 'abc_prepended_2', 'pg_default'))
		self.assertTrue(check_tablespace(node, 'abc_added_2', 'pg_default'))
		self.assertTrue(check_tablespace(node, 'abc_splitted_2', 'pg_default'))

	@if_fdw_enabled
	def test_foreign_table(self):
		"""Test foreign tables"""

		# Start master server
		master = get_new_node('test')
		master.init()
		master.append_conf(
			'postgresql.conf',
			'shared_preload_libraries=\'pg_pathman, postgres_fdw\'\n')
		master.start()
		master.psql('postgres', 'create extension pg_pathman')
		master.psql('postgres', 'create extension postgres_fdw')

		# RANGE partitioning test with FDW:
		#   - create range partitioned table in master 
		#   - create foreign server
		#   - create foreign table and insert some data into it
		#   - attach foreign table to partitioned one
		#   - try inserting data into foreign partition via parent
		#   - drop partitions
		master.psql(
			'postgres',
			'''create table abc(id serial, name text);
			select create_range_partitions('abc', 'id', 0, 10, 2)''')

		# Current user name (needed for user mapping)
		username = master.execute('postgres', 'select current_user')[0][0]

		# Start foreign server
		fserv = get_new_node('fserv')
		fserv.init().start()
		fserv.safe_psql('postgres', 'create table ftable(id serial, name text)')
		fserv.safe_psql('postgres', 'insert into ftable values (25, \'foreign\')')

		# Create foreign table and attach it to partitioned table
		master.safe_psql(
			'postgres',
			'''create server fserv
			foreign data wrapper postgres_fdw
			options (dbname 'postgres', host '127.0.0.1', port '{}')'''.format(fserv.port)
		)
		master.safe_psql(
			'postgres',
			'''create user mapping for {0}
			server fserv
			options (user '{0}')'''.format(username)
		)
		master.safe_psql(
			'postgres',
			'''import foreign schema public limit to (ftable)
			from server fserv into public'''
		)
		master.safe_psql(
			'postgres',
			'select attach_range_partition(\'abc\', \'ftable\', 20, 30)')

		# Check that table attached to partitioned table
		self.assertEqual(
			master.safe_psql('postgres', 'select * from ftable'),
			'25|foreign\n'
		)

		# Check that we can successfully insert new data into foreign partition
		master.safe_psql('postgres', 'insert into abc values (26, \'part\')')
		self.assertEqual(
			master.safe_psql('postgres', 'select * from ftable order by id'),
			'25|foreign\n26|part\n'
		)

		# Testing drop partitions (including foreign partitions)
		master.safe_psql('postgres', 'select drop_partitions(\'abc\')')

		# HASH partitioning with FDW:
		#   - create hash partitioned table in master 
		#   - create foreign table
		#   - replace local partition with foreign one
		#   - insert data
		#   - drop partitions
		master.psql(
			'postgres',
			'''create table hash_test(id serial, name text);
			select create_hash_partitions('hash_test', 'id', 2)''')
		fserv.safe_psql('postgres', 'create table f_hash_test(id serial, name text)')

		master.safe_psql(
			'postgres',
			'''import foreign schema public limit to (f_hash_test)
			from server fserv into public'''
		)
		master.safe_psql(
			'postgres',
			'select replace_hash_partition(\'hash_test_1\', \'f_hash_test\')')
		master.safe_psql('postgres', 'insert into hash_test select generate_series(1,10)')

		self.assertEqual(
			master.safe_psql('postgres', 'select * from hash_test'),
			'1|\n2|\n5|\n6|\n8|\n9|\n3|\n4|\n7|\n10|\n'
		)
		master.safe_psql('postgres', 'select drop_partitions(\'hash_test\')')

	def test_parallel_nodes(self):
		"""Test parallel queries under partitions"""

		import json

		# Init and start postgres instance with preload pg_pathman module
		node = get_new_node('test')
		node.init()
		node.append_conf(
			'postgresql.conf',
			'shared_preload_libraries=\'pg_pathman, postgres_fdw\'\n')
		node.start()

		# Check version of postgres server
		# If version < 9.6 skip all tests for parallel queries
		version = int(node.psql("postgres", "show server_version_num")[1])
		if version < 90600:
			return

		# Prepare test database
		node.psql('postgres', 'create extension pg_pathman')
		node.psql('postgres', 'create table range_partitioned as select generate_series(1, 1e4::integer) i')
		node.psql('postgres', 'alter table range_partitioned alter column i set not null')
		node.psql('postgres', 'select create_range_partitions(\'range_partitioned\', \'i\', 1, 1e3::integer)')
		node.psql('postgres', 'vacuum analyze range_partitioned')

		node.psql('postgres', 'create table hash_partitioned as select generate_series(1, 1e4::integer) i')
		node.psql('postgres', 'alter table hash_partitioned alter column i set not null')
		node.psql('postgres', 'select create_hash_partitions(\'hash_partitioned\', \'i\', 10)')
		node.psql('postgres', 'vacuum analyze hash_partitioned')

		node.psql('postgres', """
			create or replace function query_plan(query text) returns jsonb as $$
			declare
					plan jsonb;
			begin
					execute 'explain (costs off, format json)' || query into plan;
					return plan;
			end;
			$$ language plpgsql;
		""")

		# Helper function for json equality
		def ordered(obj):
			if isinstance(obj, dict):
				return sorted((k, ordered(v)) for k, v in obj.items())
			if isinstance(obj, list):
				return sorted(ordered(x) for x in obj)
			else:
				return obj

		# Test parallel select
		with node.connect() as con:
			con.execute('set max_parallel_workers_per_gather = 2')
			con.execute('set min_parallel_relation_size = 0')
			con.execute('set parallel_setup_cost = 0')
			con.execute('set parallel_tuple_cost = 0')

			# Check parallel aggregate plan
			test_query = 'select count(*) from range_partitioned where i < 1500'
			plan = con.execute('select query_plan(\'%s\')' % test_query)[0][0]
			expected = json.loads("""
			[
			  {
				"Plan": {
				  "Node Type": "Aggregate",
				  "Strategy": "Plain",
				  "Partial Mode": "Finalize",
				  "Parallel Aware": false,
				  "Plans": [
					{
					  "Node Type": "Gather",
					  "Parent Relationship": "Outer",
					  "Parallel Aware": false,
					  "Workers Planned": 2,
					  "Single Copy": false,
					  "Plans": [
						{
						  "Node Type": "Aggregate",
						  "Strategy": "Plain",
						  "Partial Mode": "Partial",
						  "Parent Relationship": "Outer",
						  "Parallel Aware": false,
						  "Plans": [
							{
							  "Node Type": "Append",
							  "Parent Relationship": "Outer",
							  "Parallel Aware": false,
							  "Plans": [
								{
								  "Node Type": "Seq Scan",
								  "Parent Relationship": "Member",
								  "Parallel Aware": true,
								  "Relation Name": "range_partitioned_2",
								  "Alias": "range_partitioned_2",
								  "Filter": "(i < 1500)"
								},
								{
								  "Node Type": "Seq Scan",
								  "Parent Relationship": "Member",
								  "Parallel Aware": true,
								  "Relation Name": "range_partitioned_1",
								  "Alias": "range_partitioned_1"
								}
							  ]
							}
						  ]
						}
					  ]
					}
				  ]
				}
			  }
			]
			""")
			self.assertEqual(ordered(plan), ordered(expected))

			# Check count of returned tuples
			count = con.execute('select count(*) from range_partitioned where i < 1500')[0][0]
			self.assertEqual(count, 1499)

			# Check simple parallel seq scan plan with limit
			test_query = 'select * from range_partitioned where i < 1500 limit 5'
			plan = con.execute('select query_plan(\'%s\')' % test_query)[0][0]
			expected = json.loads("""
			[
			  {
				"Plan": {
				  "Node Type": "Limit",
				  "Parallel Aware": false,
				  "Plans": [
					{
					  "Node Type": "Gather",
					  "Parent Relationship": "Outer",
					  "Parallel Aware": false,
					  "Workers Planned": 2,
					  "Single Copy": false,
					  "Plans": [
						{
						  "Node Type": "Append",
						  "Parent Relationship": "Outer",
						  "Parallel Aware": false,
						  "Plans": [
							{
							  "Node Type": "Seq Scan",
							  "Parent Relationship": "Member",
							  "Parallel Aware": true,
							  "Relation Name": "range_partitioned_2",
							  "Alias": "range_partitioned_2",
							  "Filter": "(i < 1500)"
							},
							{
							  "Node Type": "Seq Scan",
							  "Parent Relationship": "Member",
							  "Parallel Aware": true,
							  "Relation Name": "range_partitioned_1",
							  "Alias": "range_partitioned_1"
							}
						  ]
						}
					  ]
					}
				  ]
				}
			  }
			]
			""")
			self.assertEqual(ordered(plan), ordered(expected))

			# Check tuples returned by query above
			res_tuples = con.execute('select * from range_partitioned where i < 1500 limit 5')
			res_tuples = sorted(map(lambda x: x[0], res_tuples))
			expected = [1, 2, 3, 4, 5]
			self.assertEqual(res_tuples, expected)

			# Check the case when none partition is selected in result plan
			test_query = 'select * from range_partitioned where i < 1'
			plan = con.execute('select query_plan(\'%s\')' % test_query)[0][0]
			expected = json.loads("""
			[
			   {
				 "Plan": {
				   "Node Type": "Result",
				   "Parallel Aware": false,
				   "One-Time Filter": "false"
				 }
			   }
			]
			""")
			self.assertEqual(ordered(plan), ordered(expected))

		# Remove all objects for testing
		node.psql('postgres', 'drop table range_partitioned cascade')
		node.psql('postgres', 'drop table hash_partitioned cascade')
		node.psql('postgres', 'drop extension pg_pathman cascade')

		# Stop instance and finish work
		node.stop()
		node.cleanup()

	def test_conc_part_creation_insert(self):
		"""Test concurrent partition creation on INSERT"""

		# Create and start new instance
		node = self.start_new_pathman_cluster(allows_streaming=False)

		# Create table 'ins_test' and partition it
		with node.connect() as con0:
			con0.begin()
			con0.execute('create table ins_test(val int not null)')
			con0.execute('insert into ins_test select generate_series(1, 50)')
			con0.execute("select create_range_partitions('ins_test', 'val', 1, 10)")
			con0.commit()

		# Create two separate connections for this test
		with node.connect() as con1, node.connect() as con2:

			# Thread for connection #2 (it has to wait)
			def con2_thread():
				con2.execute('insert into ins_test values(51)')

			# Step 1: lock partitioned table in con1
			con1.begin()
			con1.execute('lock table ins_test in share update exclusive mode')

			# Step 2: try inserting new value in con2 (waiting)
			t = threading.Thread(target=con2_thread)
			t.start()

			# Step 3: try inserting new value in con1 (success, unlock)
			con1.execute('insert into ins_test values(52)')
			con1.commit()

			# Step 4: wait for con2
			t.join()

			rows = con1.execute("""
				select * from pathman_partition_list
				where parent = 'ins_test'::regclass
				order by range_min, range_max
			""")

			# check number of partitions
			self.assertEqual(len(rows), 6)

			# check range_max of partitions
			self.assertEqual(int(rows[0][5]), 11)
			self.assertEqual(int(rows[1][5]), 21)
			self.assertEqual(int(rows[2][5]), 31)
			self.assertEqual(int(rows[3][5]), 41)
			self.assertEqual(int(rows[4][5]), 51)
			self.assertEqual(int(rows[5][5]), 61)

		# Stop instance and finish work
		node.stop()
		node.cleanup()


if __name__ == "__main__":
	unittest.main()
