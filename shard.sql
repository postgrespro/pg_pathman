/* ------------------------------------------------------------------------
 *
 * shard.sql
 *		Creates shards using postgtres_fdw and pg_tsdtm
 *
 * Copyright (c) 2015-2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

/*
 * Create partitions and foreign data table for the given parent table.
 * It is necessary to create postgres_fdw foreign data servers for all shard nodes with some unique type.
 * All foreign data servers with this types will be treated as sharding nodes.
 * This function requires that the same database schema is replicated to all shards (i.e. replicated table defintion is present at all shards)
 * 
 * @param partitioned_table reference to the existed parititioned table
 * @param partition_key attribute for hash partitioning
 * @param partitions_count number of partitions
 * @param server_type some unieue name identifying servers for used for sharding of this table
 * @return nubmer of sharding nodes
 */
create or replace function @extschema@.create_shards(partitioned_table regclass, partition_key text, partitions_count integer, server_type text)
returns integer as
$$
declare
	shards text[];
	table_name text := partitioned_table::text;	
	table_namespace text;
	n_shards integer;
	create_fdw_table text := 'create foreign table "%1$s_fdw_%2$s"() inherits (%1$I) server %3$s options(table_name ''%1$s_fdw_%2$s'')';
	local_partition regclass;
	remote_partition regclass;
	nsname text;
	i integer;
begin
    PERFORM @extschema@.create_hash_partitions(partitioned_table, partition_key, partitions_count, false);

    shards = array(select srvname FROM pg_catalog.pg_foreign_server WHERE srvtype = server_type);
	n_shards := array_length(shards, 1);

	for i in 1..partitions_count loop
	    execute format(create_fdw_table, table_name, i, shards[1 + ((i-1) % n_shards)]);
		local_partition := format('%s_%s', table_name, i-1)::regclass;
		remote_partition := format('%s_fdw_%s', table_name, i)::regclass;
		select nspname from pg_catalog.pg_namespace ns,pg_class c where c.oid = remote_partition and c.relnamespace=ns.oid into table_namespace;
		perform postgres_fdw_exec(remote_partition::oid, format('create table %1$s.%2$s(like %1$s.%3$I INCLUDING ALL)', 
														 	    table_namespace, remote_partition, table_name)::cstring);
		perform @extschema@.replace_hash_partition(local_partition,remote_partition);
    end loop;
	
	return n_shards;
end
$$ LANGUAGE plpgsql;
