/* ------------------------------------------------------------------------
 *
 * hash.sql
 *		HASH partitioning functions
 *
 * Copyright (c) 2015-2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

/*
 * Creates hash partitions for specified relation
 */
CREATE OR REPLACE FUNCTION @extschema@.create_hash_partitions(
	parent_relid		REGCLASS,
	expression			TEXT,
	partitions_count	INT4,
	partition_data		BOOLEAN DEFAULT TRUE,
	partition_names		TEXT[] DEFAULT NULL,
	tablespaces			TEXT[] DEFAULT NULL)
RETURNS INTEGER AS $$
BEGIN
	PERFORM @extschema@.prepare_for_partitioning(parent_relid,
												 expression,
												 partition_data);

	/* Insert new entry to pathman config */
	PERFORM @extschema@.add_to_pathman_config(parent_relid, expression);

	/* Create partitions */
	PERFORM @extschema@.create_hash_partitions_internal(parent_relid,
														expression,
														partitions_count,
														partition_names,
														tablespaces);

	/* Copy data */
	IF partition_data = true THEN
		PERFORM @extschema@.set_enable_parent(parent_relid, false);
		PERFORM @extschema@.partition_data(parent_relid);
	ELSE
		PERFORM @extschema@.set_enable_parent(parent_relid, true);
	END IF;

	RETURN partitions_count;
END
$$ LANGUAGE plpgsql
SET client_min_messages = WARNING;

/*
 * Replace hash partition with another one. It could be useful in case when
 * someone wants to attach foreign table as a partition.
 *
 * lock_parent - should we take an exclusive lock?
 */
CREATE OR REPLACE FUNCTION @extschema@.replace_hash_partition(
	old_partition		REGCLASS,
	new_partition		REGCLASS,
	lock_parent			BOOL DEFAULT TRUE)
RETURNS REGCLASS AS $$
DECLARE
	parent_relid		REGCLASS;
	old_constr_name		TEXT;		/* name of old_partition's constraint */
	old_constr_def		TEXT;		/* definition of old_partition's constraint */
	rel_persistence		CHAR;
	p_init_callback		REGPROCEDURE;

BEGIN
	PERFORM @extschema@.validate_relname(old_partition);
	PERFORM @extschema@.validate_relname(new_partition);

	/* Parent relation */
	parent_relid := @extschema@.get_parent_of_partition(old_partition);

	IF lock_parent THEN
		/* Acquire data modification lock (prevent further modifications) */
		PERFORM @extschema@.prevent_data_modification(parent_relid);
	ELSE
		/* Acquire lock on parent */
		PERFORM @extschema@.prevent_part_modification(parent_relid);
	END IF;

	/* Acquire data modification lock (prevent further modifications) */
	PERFORM @extschema@.prevent_data_modification(old_partition);
	PERFORM @extschema@.prevent_data_modification(new_partition);

	/* Ignore temporary tables */
	SELECT relpersistence FROM pg_catalog.pg_class
	WHERE oid = new_partition INTO rel_persistence;

	IF rel_persistence = 't'::CHAR THEN
		RAISE EXCEPTION 'temporary table "%" cannot be used as a partition',
						new_partition::TEXT;
	END IF;

	/* Check that new partition has an equal structure as parent does */
	IF NOT @extschema@.is_tuple_convertible(parent_relid, new_partition) THEN
		RAISE EXCEPTION 'partition must have a compatible tuple format';
	END IF;

	/* Check that table is partitioned */
	IF @extschema@.get_partition_key(parent_relid) IS NULL THEN
		RAISE EXCEPTION 'table "%" is not partitioned', parent_relid::TEXT;
	END IF;

	/* Fetch name of old_partition's HASH constraint */
	old_constr_name = @extschema@.build_check_constraint_name(old_partition::REGCLASS);

	/* Fetch definition of old_partition's HASH constraint */
	SELECT pg_catalog.pg_get_constraintdef(oid) FROM pg_catalog.pg_constraint
	WHERE conrelid = old_partition AND conname = old_constr_name
	INTO old_constr_def;

	/* Detach old partition */
	EXECUTE format('ALTER TABLE %s NO INHERIT %s', old_partition, parent_relid);
	EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %s',
				   old_partition,
				   old_constr_name);

	/* Attach the new one */
	EXECUTE format('ALTER TABLE %s INHERIT %s', new_partition, parent_relid);
	EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %s %s',
				   new_partition,
				   @extschema@.build_check_constraint_name(new_partition::REGCLASS),
				   old_constr_def);

	/* Fetch init_callback from 'params' table */
	WITH stub_callback(stub) as (values (0))
	SELECT init_callback
	FROM stub_callback
	LEFT JOIN @extschema@.pathman_config_params AS params
	ON params.partrel = parent_relid
	INTO p_init_callback;

	/* Finally invoke init_callback */
	PERFORM @extschema@.invoke_on_partition_created_callback(parent_relid,
															 new_partition,
															 p_init_callback);

	RETURN new_partition;
END
$$ LANGUAGE plpgsql;

/*
 * Just create HASH partitions, called by create_hash_partitions().
 */
CREATE OR REPLACE FUNCTION @extschema@.create_hash_partitions_internal(
	parent_relid		REGCLASS,
	attribute			TEXT,
	partitions_count	INT4,
	partition_names		TEXT[] DEFAULT NULL,
	tablespaces			TEXT[] DEFAULT NULL)
RETURNS VOID AS 'pg_pathman', 'create_hash_partitions_internal'
LANGUAGE C;

/*
 * Calculates hash for integer value
 */
CREATE OR REPLACE FUNCTION @extschema@.get_hash_part_idx(INT4, INT4)
RETURNS INTEGER AS 'pg_pathman', 'get_hash_part_idx'
LANGUAGE C STRICT;

/*
 * Build hash condition for a CHECK CONSTRAINT
 */
CREATE OR REPLACE FUNCTION @extschema@.build_hash_condition(
	attribute_type		REGTYPE,
	attribute			TEXT,
	partitions_count	INT4,
	partition_index		INT4)
RETURNS TEXT AS 'pg_pathman', 'build_hash_condition'
LANGUAGE C STRICT;
