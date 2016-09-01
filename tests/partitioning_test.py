#coding: utf-8
"""
 concurrent_partitioning_test.py
        Tests concurrent partitioning worker with simultaneous update queries

 Copyright (c) 2015-2016, Postgres Professional
"""

import unittest
from testgres import get_new_node, clean_all, stop_all
from subprocess import Popen, PIPE
import subprocess
import time


class PartitioningTests(unittest.TestCase):

	def setUp(self):
		self.setup_cmd = [
			'create extension pg_pathman',
			'create table abc(id serial, t text)',
			'insert into abc select generate_series(1, 300000)',
			'select create_hash_partitions(\'abc\', \'id\', 3, partition_data := false)',
		]

	def tearDown(self):
		stop_all()
		# clean_all()

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
				print line

	def test_concurrent(self):
		"""Tests concurrent partitioning"""
		node = get_new_node('test')
		try:
			node.init()
			node.append_conf('postgresql.conf', 'shared_preload_libraries=\'pg_pathman\'\n')
			node.start()
			self.init_test_data(node)

			node.psql('postgres', 'select partition_table_concurrently(\'abc\')')

			while True:
				# update some rows to check for deadlocks
				# import ipdb; ipdb.set_trace()
				node.safe_psql('postgres', 
					'''
						update abc set t = 'test'
						where id in (select (random() * 300000)::int from generate_series(1, 3000))
					''')

				count = node.execute('postgres', 'select count(*) from pathman_concurrent_part_tasks')

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
			node.init(allows_streaming=True)
			node.append_conf('postgresql.conf', 'shared_preload_libraries=\'pg_pathman\'\n')
			node.start()
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
			node.append_conf('postgresql.conf', 'shared_preload_libraries=\'pg_pathman\'\n')
			node.start()
			node.safe_psql(
				'postgres',
				'create extension pg_pathman; '
				+ 'create table abc(id serial, t text); '
				+ 'insert into abc select generate_series(1, 100000); '
				+ 'select create_range_partitions(\'abc\', \'id\', 1, 50000);'
			)

			# Start transaction that will create partition
			con = node.connect()
			con.begin()
			con.execute('select append_range_partition(\'abc\')')

			# Start threads that suppose to add new partitions and wait some time
			query = [
				'select prepend_range_partition(\'abc\')',
				'select append_range_partition(\'abc\')',
				'select add_range_partition(\'abc\', 500000, 550000)',
			]
			threads = []
			for i in range(3):
				thread = \
					threading.Thread(target=add_partition, args=(node, flags[i], query[i]))
				threads.append(thread)
				thread.start()
			time.sleep(3)

			# This threads should wait until current transaction finished
			with lock:
				for i in range(3):
					self.assertEqual(flags[i].get(), False)

			# Commit transaction. Since then other sessions can create partitions
			con.commit()

			# Now wait until each thread finishes
			for i in range(3):
				threads[i].join()

			# Check flags, it should be true which means that threads are finished
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

if __name__ == "__main__":
    unittest.main()
