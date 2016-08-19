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


class ConcurrentTest(unittest.TestCase):

	def setUp(self):
		pass

	def tearDown(self):
		stop_all()
		# clean_all()

	def test_concurrent(self):
		setup_cmd = [
			'create extension pg_pathman',
			'create table abc(id serial, t text)',
			'insert into abc select generate_series(1, 300000)',
			'select create_hash_partitions(\'abc\', \'id\', 3, p_partition_data := false)',
		]

		node = get_new_node('test')
		node.init()
		node.append_conf('postgresql.conf', 'shared_preload_libraries=\'pg_pathman\'\n')
		node.start()

		for cmd in setup_cmd:
			node.safe_psql('postgres', cmd)

		node.psql('postgres', 'select partition_data_worker(\'abc\')')

		while True:
			# update some rows to check for deadlocks
			node.safe_psql('postgres', 
				'''update abc set t = 'test'
				where id in (select (random() * 300000)::int from generate_series(1, 3000))''')

			count = node.execute('postgres', 'select count(*) from pathman_active_workers')

			# if there is no active workers then it means work is done
			if count[0][0] == 0:
				break
			time.sleep(1)

		data = node.execute('postgres', 'select count(*) from only abc')
		self.assertEqual(data[0][0], 0)
		data = node.execute('postgres', 'select count(*) from abc')
		self.assertEqual(data[0][0], 300000)

		node.stop()

if __name__ == "__main__":
    unittest.main()
