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
            # 'create extension pg_pathman',
            'create table abc(id serial, t text)',
            'insert into abc select generate_series(1, 300000)',
            'select create_hash_partitions(\'abc\', \'id\', 3, partition_data := false)',
        ]

    def tearDown(self):
        stop_all()
        # clean_all()

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
                'select get_rel_tablespace_name(\'{}\')'.format(tablename))
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
        self.assertTrue(check_tablespace(node, 'abc_appended_2', 'pg_default'))
        self.assertTrue(check_tablespace(node, 'abc_prepended_2', 'pg_default'))
        self.assertTrue(check_tablespace(node, 'abc_added_2', 'pg_default'))

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

    def test_parallel_nodes(self):
        """Test parallel queries under partitions"""

        # Init and start postgres instance with preload pg_pathman module
        node = get_new_node('test')
        node.init()
        node.append_conf(
            'postgresql.conf',
            'shared_preload_libraries=\'pg_pathman, postgres_fdw\'\n')
        node.start()

        # Check version of postgres server
        # If version < 9.6 skip all tests for parallel queries
        version = node.execute("postgres", "show server_version_num")
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

        # Test parallel select
        with node.connect() as con:
            con.execute('set max_parallel_workers_per_gather = 2')
            con.execute('set min_parallel_relation_size = 0')
            con.execute('set parallel_setup_cost = 0')
            con.execute('set parallel_tuple_cost = 0')

            # Check parallel aggregate plan
            plan = con.execute('explain (costs off) select count(*) from range_partitioned where i < 1500')
            expected = [('Finalize Aggregate',),
                        ('  ->  Gather',),
                        ('        Workers Planned: 2',),
                        ('        ->  Partial Aggregate',),
                        ('              ->  Append',),
                        ('                    ->  Parallel Seq Scan on range_partitioned_1',),
                        ('                    ->  Parallel Seq Scan on range_partitioned_2',),
                        ('                          Filter: (i < 1500)',)]
            self.assertEqual(plan, expected)

            # Check count of returned tuples
            count = con.execute('select count(*) from range_partitioned where i < 1500')
            self.assertEqual(count[0][0], 1499)

            # Check simple parallel seq scan plan with limit
            plan = con.execute('explain (costs off) select * from range_partitioned where i < 1500 limit 5')
            expected = [('Limit',),
                        ('  ->  Gather',),
                        ('        Workers Planned: 2',),
                        ('        ->  Append',),
                        ('              ->  Parallel Seq Scan on range_partitioned_1',),
                        ('              ->  Parallel Seq Scan on range_partitioned_2',),
                        ('                    Filter: (i < 1500)',)]
            self.assertEqual(plan, expected)

            # Check tuples returned by query above
            res_tuples = con.execute('select * from range_partitioned where i < 1500 limit 5')
            expected = [(1,), (2,), (3,), (4,), (5,)]
            self.assertEqual(res_tuples, expected)
            #  import ipdb; ipdb.set_trace()

        # Remove all objects for testing
        node.psql('postgres', 'drop table range_partitioned cascade')
        node.psql('postgres', 'drop table hash_partitioned cascade')
        node.psql('postgres', 'drop extension pg_pathman cascade')

        # Stop instance and finish work
        node.stop()
        node.cleanup()


if __name__ == "__main__":
    unittest.main()
