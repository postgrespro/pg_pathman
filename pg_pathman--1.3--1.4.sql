/* ------------------------------------------------------------------------
 *
 * pg_pathman--1.3--1.4.sql
 *		Migration scripts to version 1.4
 *
 * Copyright (c) 2015-2017, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */


/* ------------------------------------------------------------------------
 * Alter config tables
 * ----------------------------------------------------------------------*/
ALTER TABLE @extschema@.pathman_config RENAME COLUMN attname TO expr;
ALTER TABLE @extschema@.pathman_config ADD COLUMN cooked_expr TEXT;

DROP TRIGGER pathman_config_params_trigger ON @extschema@.pathman_config_params;

CREATE TRIGGER pathman_config_params_trigger
AFTER INSERT OR UPDATE OR DELETE ON @extschema@.pathman_config_params
FOR EACH ROW EXECUTE PROCEDURE @extschema@.pathman_config_params_trigger_func();


DROP FUNCTION @extschema@.validate_interval_value(REGCLASS, TEXT, INTEGER, TEXT) CASCADE;

CREATE OR REPLACE FUNCTION @extschema@.validate_interval_value(
	partrel			REGCLASS,
	expr			TEXT,
	parttype		INTEGER,
	range_interval	TEXT,
	cooked_expr		TEXT)
RETURNS BOOL AS 'pg_pathman', 'validate_interval_value'
LANGUAGE C;

ALTER TABLE @extschema@.pathman_config
ADD CONSTRAINT pathman_config_interval_check 
	CHECK (@extschema@.validate_interval_value(partrel,
											   expr,
											   parttype,
											   range_interval,
											   cooked_expr));

DO $$
DECLARE
	v_rec RECORD;
BEGIN
	FOR v_rec IN (SELECT conrelid::regclass AS t, conname, regexp_replace(conname, '\d+_check', 'check') as new_conname
				  FROM pg_constraint
				  WHERE conname ~ 'pathman_.*_\d+_\d+_check')
	LOOP
		EXECUTE format('ALTER TABLE %s RENAME CONSTRAINT %s TO %s',
			v_rec.t, v_rec.conname, v_rec.new_conname);
	END LOOP;
END
$$ LANGUAGE plpgsql;


DROP VIEW pathman_partition_list;

DROP FUNCTION @extschema@.show_partition_list();

CREATE OR REPLACE FUNCTION @extschema@.show_partition_list()
RETURNS TABLE (
	parent			REGCLASS,
	partition		REGCLASS,
	parttype		INT4,
	expr			TEXT,
	range_min		TEXT,
	range_max		TEXT)
AS 'pg_pathman', 'show_partition_list_internal'
LANGUAGE C STRICT;

CREATE OR REPLACE VIEW @extschema@.pathman_partition_list
AS SELECT * FROM @extschema@.show_partition_list();

GRANT SELECT ON @extschema@.pathman_partition_list TO PUBLIC;


/* ------------------------------------------------------------------------
 * Drop irrelevant objects
 * ----------------------------------------------------------------------*/
DROP FUNCTION @extschema@._partition_data_concurrent(REGCLASS, ANYELEMENT, ANYELEMENT, INT, OUT BIGINT);
DROP FUNCTION @extschema@.disable_pathman_for(REGCLASS);
DROP FUNCTION @extschema@.common_relation_checks(REGCLASS, TEXT);
DROP FUNCTION @extschema@.validate_relations_equality(OID, OID);
DROP FUNCTION @extschema@.drop_partitions(REGCLASS, BOOLEAN);
DROP FUNCTION @extschema@.on_create_partitions(REGCLASS);
DROP FUNCTION @extschema@.on_update_partitions(REGCLASS);
DROP FUNCTION @extschema@.on_remove_partitions(REGCLASS);
DROP FUNCTION @extschema@.is_attribute_nullable(REGCLASS, TEXT);
DROP FUNCTION @extschema@.build_check_constraint_name(REGCLASS, INT2);
DROP FUNCTION @extschema@.add_to_pathman_config(REGCLASS, TEXT, TEXT);
DROP FUNCTION @extschema@.lock_partitioned_relation(REGCLASS);
DROP FUNCTION @extschema@.prevent_relation_modification(REGCLASS);
DROP FUNCTION @extschema@.create_hash_partitions(REGCLASS, TEXT, INTEGER, BOOLEAN, TEXT[], TEXT[]);
DROP FUNCTION @extschema@.create_hash_update_trigger(REGCLASS);
DROP FUNCTION @extschema@.get_type_hash_func(REGTYPE);
DROP FUNCTION @extschema@.build_hash_condition(REGTYPE, TEXT, INT4, INT4);
DROP FUNCTION @extschema@.create_or_replace_sequence(REGCLASS, OUT TEXT);
DROP FUNCTION @extschema@.check_boundaries(REGCLASS, TEXT, ANYELEMENT, ANYELEMENT);
DROP FUNCTION @extschema@.create_range_partitions(REGCLASS, TEXT, ANYELEMENT, INTERVAL, INTEGER, BOOLEAN);
DROP FUNCTION @extschema@.create_range_partitions(REGCLASS, TEXT, ANYELEMENT, ANYELEMENT, INTEGER, BOOLEAN);
DROP FUNCTION @extschema@.create_partitions_from_range(REGCLASS, TEXT, ANYELEMENT, ANYELEMENT, ANYELEMENT, BOOLEAN);
DROP FUNCTION @extschema@.create_partitions_from_range(REGCLASS, TEXT, ANYELEMENT, ANYELEMENT, INTERVAL, BOOLEAN);
DROP FUNCTION @extschema@.create_range_update_trigger(REGCLASS);
DROP FUNCTION @extschema@.build_range_condition(REGCLASS, TEXT, ANYELEMENT, ANYELEMENT);
DROP FUNCTION @extschema@.find_or_create_range_partition(REGCLASS, ANYELEMENT);


/* ------------------------------------------------------------------------
 * Alter functions' modifiers
 * ----------------------------------------------------------------------*/
ALTER FUNCTION @extschema@.build_sequence_name(REGCLASS) STRICT;


/* ------------------------------------------------------------------------
 * (Re)create functions
 * ----------------------------------------------------------------------*/
CREATE OR REPLACE FUNCTION @extschema@.show_cache_stats()
RETURNS TABLE (
	context			TEXT,
	size			INT8,
	used			INT8,
	entries			INT8)
AS 'pg_pathman', 'show_cache_stats_internal'
LANGUAGE C STRICT;

CREATE OR REPLACE VIEW @extschema@.pathman_cache_stats
AS SELECT * FROM @extschema@.show_cache_stats();


CREATE OR REPLACE FUNCTION @extschema@._partition_data_concurrent(
	relation		REGCLASS,
	p_min			ANYELEMENT DEFAULT NULL::text,
	p_max			ANYELEMENT DEFAULT NULL::text,
	p_limit			INT DEFAULT NULL,
	OUT p_total		BIGINT)
AS $$
DECLARE
	part_expr		TEXT;
	v_limit_clause	TEXT := '';
	v_where_clause	TEXT := '';
	ctids			TID[];

BEGIN
	part_expr := @extschema@.get_partition_key(relation);

	p_total := 0;

	/* Format LIMIT clause if needed */
	IF NOT p_limit IS NULL THEN
		v_limit_clause := format('LIMIT %s', p_limit);
	END IF;

	/* Format WHERE clause if needed */
	IF NOT p_min IS NULL THEN
		v_where_clause := format('%1$s >= $1', part_expr);
	END IF;

	IF NOT p_max IS NULL THEN
		IF NOT p_min IS NULL THEN
			v_where_clause := v_where_clause || ' AND ';
		END IF;
		v_where_clause := v_where_clause || format('%1$s < $2', part_expr);
	END IF;

	IF v_where_clause != '' THEN
		v_where_clause := 'WHERE ' || v_where_clause;
	END IF;

	/* Lock rows and copy data */
	RAISE NOTICE 'Copying data to partitions...';
	EXECUTE format('SELECT array(SELECT ctid FROM ONLY %1$s %2$s %3$s FOR UPDATE NOWAIT)',
				   relation, v_where_clause, v_limit_clause)
	USING p_min, p_max
	INTO ctids;

	EXECUTE format('WITH data AS (
					DELETE FROM ONLY %1$s WHERE ctid = ANY($1) RETURNING *)
					INSERT INTO %1$s SELECT * FROM data',
				   relation)
	USING ctids;

	/* Get number of inserted rows */
	GET DIAGNOSTICS p_total = ROW_COUNT;
	RETURN;
END
$$ LANGUAGE plpgsql
SET pg_pathman.enable_partitionfilter = on; /* ensures that PartitionFilter is ON */


CREATE OR REPLACE FUNCTION @extschema@.disable_pathman_for(
	parent_relid	REGCLASS)
RETURNS VOID AS $$
BEGIN
	PERFORM @extschema@.validate_relname(parent_relid);

	/* Delete rows from both config tables */
	DELETE FROM @extschema@.pathman_config WHERE partrel = parent_relid;
	DELETE FROM @extschema@.pathman_config_params WHERE partrel = parent_relid;

	/* Drop triggers on update */
	PERFORM @extschema@.drop_triggers(parent_relid);
END
$$ LANGUAGE plpgsql STRICT;


CREATE OR REPLACE FUNCTION @extschema@.prepare_for_partitioning(
	parent_relid	REGCLASS,
	expression		TEXT,
	partition_data	BOOLEAN)
RETURNS VOID AS $$
DECLARE
	constr_name		TEXT;
	is_referenced	BOOLEAN;
	rel_persistence	CHAR;

BEGIN
	PERFORM @extschema@.validate_relname(parent_relid);
	PERFORM @extschema@.validate_expression(parent_relid, expression);

	IF partition_data = true THEN
		/* Acquire data modification lock */
		PERFORM @extschema@.prevent_data_modification(parent_relid);
	ELSE
		/* Acquire lock on parent */
		PERFORM @extschema@.prevent_part_modification(parent_relid);
	END IF;

	/* Ignore temporary tables */
	SELECT relpersistence FROM pg_catalog.pg_class
	WHERE oid = parent_relid INTO rel_persistence;

	IF rel_persistence = 't'::CHAR THEN
		RAISE EXCEPTION 'temporary table "%" cannot be partitioned', parent_relid;
	END IF;

	IF EXISTS (SELECT * FROM @extschema@.pathman_config
			   WHERE partrel = parent_relid) THEN
		RAISE EXCEPTION 'table "%" has already been partitioned', parent_relid;
	END IF;

	/* Check if there are foreign keys that reference the relation */
	FOR constr_name IN (SELECT conname FROM pg_catalog.pg_constraint
					WHERE confrelid = parent_relid::REGCLASS::OID)
	LOOP
		is_referenced := TRUE;
		RAISE WARNING 'foreign key "%" references table "%"', constr_name, parent_relid;
	END LOOP;

	IF is_referenced THEN
		RAISE EXCEPTION 'table "%" is referenced from other tables', parent_relid;
	END IF;

END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION @extschema@.create_naming_sequence(
	parent_relid	REGCLASS)
RETURNS TEXT AS $$
DECLARE
	seq_name		TEXT;

BEGIN
	seq_name := @extschema@.build_sequence_name(parent_relid);

	EXECUTE format('DROP SEQUENCE IF EXISTS %s', seq_name);
	EXECUTE format('CREATE SEQUENCE %s START 1', seq_name);

	RETURN seq_name;
END
$$ LANGUAGE plpgsql
SET client_min_messages = WARNING; /* mute NOTICE message */


CREATE OR REPLACE FUNCTION @extschema@.drop_naming_sequence(
	parent_relid	REGCLASS)
RETURNS VOID AS $$
DECLARE
	seq_name		TEXT;

BEGIN
	seq_name := @extschema@.build_sequence_name(parent_relid);

	EXECUTE format('DROP SEQUENCE IF EXISTS %s', seq_name);
END
$$ LANGUAGE plpgsql
SET client_min_messages = WARNING; /* mute NOTICE message */


CREATE OR REPLACE FUNCTION @extschema@.drop_triggers(
	parent_relid	REGCLASS)
RETURNS VOID AS $$
DECLARE
	triggername		TEXT;
	relation		OID;

BEGIN
	triggername := @extschema@.build_update_trigger_name(parent_relid);

	/* Drop trigger for each partition if exists */
	FOR relation IN (SELECT pg_catalog.pg_inherits.inhrelid
					 FROM pg_catalog.pg_inherits
					 JOIN pg_catalog.pg_trigger ON inhrelid = tgrelid
					 WHERE inhparent = parent_relid AND tgname = triggername)
	LOOP
		EXECUTE format('DROP TRIGGER IF EXISTS %s ON %s',
					   triggername,
					   relation::REGCLASS);
	END LOOP;

	/* Drop trigger on parent */
	IF EXISTS (SELECT * FROM pg_catalog.pg_trigger
			   WHERE tgname = triggername AND tgrelid = parent_relid)
	THEN
		EXECUTE format('DROP TRIGGER IF EXISTS %s ON %s',
					   triggername,
					   parent_relid::TEXT);
	END IF;
END
$$ LANGUAGE plpgsql STRICT;


CREATE OR REPLACE FUNCTION @extschema@.drop_partitions(
	parent_relid	REGCLASS,
	delete_data		BOOLEAN DEFAULT FALSE)
RETURNS INTEGER AS $$
DECLARE
	child			REGCLASS;
	rows_count		BIGINT;
	part_count		INTEGER := 0;
	rel_kind		CHAR;

BEGIN
	PERFORM @extschema@.validate_relname(parent_relid);

	/* Acquire data modification lock */
	PERFORM @extschema@.prevent_data_modification(parent_relid);

	IF NOT EXISTS (SELECT FROM @extschema@.pathman_config
				   WHERE partrel = parent_relid) THEN
		RAISE EXCEPTION 'table "%" has no partitions', parent_relid::TEXT;
	END IF;

	/* First, drop all triggers */
	PERFORM @extschema@.drop_triggers(parent_relid);

	/* Also drop naming sequence */
	PERFORM @extschema@.drop_naming_sequence(parent_relid);

	FOR child IN (SELECT inhrelid::REGCLASS
				  FROM pg_catalog.pg_inherits
				  WHERE inhparent::regclass = parent_relid
				  ORDER BY inhrelid ASC)
	LOOP
		IF NOT delete_data THEN
			EXECUTE format('INSERT INTO %s SELECT * FROM %s',
							parent_relid::TEXT,
							child::TEXT);
			GET DIAGNOSTICS rows_count = ROW_COUNT;

			/* Show number of copied rows */
			RAISE NOTICE '% rows copied from %', rows_count, child;
		END IF;

		SELECT relkind FROM pg_catalog.pg_class
		WHERE oid = child
		INTO rel_kind;

		/*
		 * Determine the kind of child relation. It can be either a regular
		 * table (r) or a foreign table (f). Depending on relkind we use
		 * DROP TABLE or DROP FOREIGN TABLE.
		 */
		IF rel_kind = 'f' THEN
			EXECUTE format('DROP FOREIGN TABLE %s', child);
		ELSE
			EXECUTE format('DROP TABLE %s', child);
		END IF;

		part_count := part_count + 1;
	END LOOP;

	/* Finally delete both config entries */
	DELETE FROM @extschema@.pathman_config WHERE partrel = parent_relid;
	DELETE FROM @extschema@.pathman_config_params WHERE partrel = parent_relid;

	RETURN part_count;
END
$$ LANGUAGE plpgsql
SET pg_pathman.enable_partitionfilter = off; /* ensures that PartitionFilter is OFF */


CREATE OR REPLACE FUNCTION @extschema@.copy_foreign_keys(
	parent_relid	REGCLASS,
	partition_relid	REGCLASS)
RETURNS VOID AS $$
DECLARE
	conid			OID;

BEGIN
	PERFORM @extschema@.validate_relname(parent_relid);
	PERFORM @extschema@.validate_relname(partition_relid);

	FOR conid IN (SELECT oid FROM pg_catalog.pg_constraint
				  WHERE conrelid = parent_relid AND contype = 'f')
	LOOP
		EXECUTE format('ALTER TABLE %s ADD %s',
					   partition_relid::TEXT,
					   pg_catalog.pg_get_constraintdef(conid));
	END LOOP;
END
$$ LANGUAGE plpgsql STRICT;


CREATE OR REPLACE FUNCTION @extschema@.get_partition_key(
	relid	REGCLASS)
RETURNS TEXT AS
$$
	SELECT expr FROM @extschema@.pathman_config WHERE partrel = relid;
$$
LANGUAGE sql STRICT;


CREATE OR REPLACE FUNCTION @extschema@.get_partition_type(
	relid	REGCLASS)
RETURNS INT4 AS
$$
	SELECT parttype FROM @extschema@.pathman_config WHERE partrel = relid;
$$
LANGUAGE sql STRICT;


CREATE OR REPLACE FUNCTION @extschema@.pathman_update_trigger_func()
RETURNS TRIGGER AS 'pg_pathman', 'pathman_update_trigger_func'
LANGUAGE C STRICT;


CREATE OR REPLACE FUNCTION @extschema@.create_update_triggers(
	parent_relid	REGCLASS)
RETURNS VOID AS 'pg_pathman', 'create_update_triggers'
LANGUAGE C STRICT;


CREATE OR REPLACE FUNCTION @extschema@.create_single_update_trigger(
	parent_relid	REGCLASS,
	partition_relid	REGCLASS)
RETURNS VOID AS 'pg_pathman', 'create_single_update_trigger'
LANGUAGE C STRICT;


CREATE OR REPLACE FUNCTION @extschema@.has_update_trigger(
	parent_relid	REGCLASS)
RETURNS BOOL AS 'pg_pathman', 'has_update_trigger'
LANGUAGE C STRICT;


CREATE OR REPLACE FUNCTION @extschema@.validate_expression(
	relid	REGCLASS,
	expression TEXT)
RETURNS VOID AS 'pg_pathman', 'validate_expression'
LANGUAGE C;


CREATE OR REPLACE FUNCTION @extschema@.is_operator_supported(
	type_oid	REGTYPE,
	opname		TEXT)
RETURNS BOOLEAN AS 'pg_pathman', 'is_operator_supported'
LANGUAGE C STRICT;


CREATE OR REPLACE FUNCTION @extschema@.is_tuple_convertible(
	relation1	REGCLASS,
	relation2	REGCLASS)
RETURNS BOOL AS 'pg_pathman', 'is_tuple_convertible'
LANGUAGE C STRICT;


CREATE OR REPLACE FUNCTION @extschema@.build_check_constraint_name(
	partition_relid	REGCLASS)
RETURNS TEXT AS 'pg_pathman', 'build_check_constraint_name'
LANGUAGE C STRICT;


CREATE OR REPLACE FUNCTION @extschema@.add_to_pathman_config(
	parent_relid		REGCLASS,
	expression			TEXT,
	range_interval		TEXT)
RETURNS BOOLEAN AS 'pg_pathman', 'add_to_pathman_config'
LANGUAGE C;


CREATE OR REPLACE FUNCTION @extschema@.add_to_pathman_config(
	parent_relid		REGCLASS,
	expression			TEXT)
RETURNS BOOLEAN AS 'pg_pathman', 'add_to_pathman_config'
LANGUAGE C;


CREATE OR REPLACE FUNCTION @extschema@.prevent_part_modification(
	parent_relid	REGCLASS)
RETURNS VOID AS 'pg_pathman', 'prevent_part_modification'
LANGUAGE C STRICT;


CREATE OR REPLACE FUNCTION @extschema@.prevent_data_modification(
	parent_relid	REGCLASS)
RETURNS VOID AS 'pg_pathman', 'prevent_data_modification'
LANGUAGE C STRICT;


CREATE OR REPLACE FUNCTION @extschema@.create_hash_partitions(
	parent_relid		REGCLASS,
	expression			TEXT,
	partitions_count	INT4,
	partition_data		BOOLEAN DEFAULT TRUE,
	partition_names		TEXT[] DEFAULT NULL,
	tablespaces			TEXT[] DEFAULT NULL)
RETURNS INTEGER AS $$
BEGIN
	expression := lower(expression);
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


CREATE OR REPLACE FUNCTION @extschema@.build_hash_condition(
	attribute_type		REGTYPE,
	attribute			TEXT,
	partitions_count	INT4,
	partition_index		INT4)
RETURNS TEXT AS 'pg_pathman', 'build_hash_condition'
LANGUAGE C STRICT;


CREATE OR REPLACE FUNCTION @extschema@.check_boundaries(
	parent_relid	REGCLASS,
	expression		TEXT,
	start_value		ANYELEMENT,
	end_value		ANYELEMENT)
RETURNS VOID AS $$
DECLARE
	min_value		start_value%TYPE;
	max_value		start_value%TYPE;
	rows_count		BIGINT;

BEGIN
	/* Get min and max values */
	EXECUTE format('SELECT count(*), min(%1$s), max(%1$s)
					FROM %2$s WHERE NOT %1$s IS NULL',
				   expression, parent_relid::TEXT)
	INTO rows_count, min_value, max_value;

	/* Check if column has NULL values */
	IF rows_count > 0 AND (min_value IS NULL OR max_value IS NULL) THEN
		RAISE EXCEPTION 'expression "%" returns NULL values', expression;
	END IF;

	/* Check lower boundary */
	IF start_value > min_value THEN
		RAISE EXCEPTION 'start value is greater than min value of "%"', expression;
	END IF;

	/* Check upper boundary */
	IF end_value <= max_value THEN
		RAISE EXCEPTION 'not enough partitions to fit all values of "%"', expression;
	END IF;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION @extschema@.create_range_partitions(
	parent_relid	REGCLASS,
	expression		TEXT,
	start_value		ANYELEMENT,
	p_interval		INTERVAL,
	p_count			INTEGER DEFAULT NULL,
	partition_data	BOOLEAN DEFAULT TRUE)
RETURNS INTEGER AS $$
DECLARE
	rows_count		BIGINT;
	value_type		REGTYPE;
	max_value		start_value%TYPE;
	cur_value		start_value%TYPE := start_value;
	end_value		start_value%TYPE;
	part_count		INTEGER := 0;
	i				INTEGER;

BEGIN
	PERFORM @extschema@.prepare_for_partitioning(parent_relid,
												 expression,
												 partition_data);

	IF p_count < 0 THEN
		RAISE EXCEPTION '"p_count" must not be less than 0';
	END IF;

	/* Try to determine partitions count if not set */
	IF p_count IS NULL THEN
		EXECUTE format('SELECT count(*), max(%s) FROM %s', expression, parent_relid)
		INTO rows_count, max_value;

		IF rows_count = 0 THEN
			RAISE EXCEPTION 'cannot determine partitions count for empty table';
		END IF;

		p_count := 0;
		WHILE cur_value <= max_value
		LOOP
			cur_value := cur_value + p_interval;
			p_count := p_count + 1;
		END LOOP;
	END IF;

	value_type := @extschema@.get_base_type(pg_typeof(start_value));

	/*
	 * In case when user doesn't want to automatically create partitions
	 * and specifies partition count as 0 then do not check boundaries
	 */
	IF p_count != 0 THEN
		/* compute right bound of partitioning through additions */
		end_value := start_value;
		FOR i IN 1..p_count
		LOOP
			end_value := end_value + p_interval;
		END LOOP;

		/* Check boundaries */
		EXECUTE
			format('SELECT @extschema@.check_boundaries(''%s'', $1, ''%s'', ''%s''::%s)',
				   parent_relid,
				   start_value,
				   end_value,
				   value_type::TEXT)
		USING
			expression;
	END IF;

	/* Create sequence for child partitions names */
	PERFORM @extschema@.create_naming_sequence(parent_relid);

	/* Insert new entry to pathman config */
	PERFORM @extschema@.add_to_pathman_config(parent_relid, expression,
											  p_interval::TEXT);

	IF p_count != 0 THEN
		part_count := @extschema@.create_range_partitions_internal(
									parent_relid,
									@extschema@.generate_range_bounds(start_value,
																	  p_interval,
																	  p_count),
									NULL,
									NULL);
	END IF;

	/* Relocate data if asked to */
	IF partition_data = true THEN
		PERFORM @extschema@.set_enable_parent(parent_relid, false);
		PERFORM @extschema@.partition_data(parent_relid);
	ELSE
		PERFORM @extschema@.set_enable_parent(parent_relid, true);
	END IF;

	RETURN part_count;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION @extschema@.create_range_partitions(
	parent_relid	REGCLASS,
	expression		TEXT,
	start_value		ANYELEMENT,
	p_interval		ANYELEMENT,
	p_count			INTEGER DEFAULT NULL,
	partition_data	BOOLEAN DEFAULT TRUE)
RETURNS INTEGER AS $$
DECLARE
	rows_count		BIGINT;
	max_value		start_value%TYPE;
	cur_value		start_value%TYPE := start_value;
	end_value		start_value%TYPE;
	part_count		INTEGER := 0;
	i				INTEGER;

BEGIN
	PERFORM @extschema@.prepare_for_partitioning(parent_relid,
												 expression,
												 partition_data);

	IF p_count < 0 THEN
		RAISE EXCEPTION 'partitions count must not be less than zero';
	END IF;

	/* Try to determine partitions count if not set */
	IF p_count IS NULL THEN
		EXECUTE format('SELECT count(*), max(%s) FROM %s', expression, parent_relid)
		INTO rows_count, max_value;

		IF rows_count = 0 THEN
			RAISE EXCEPTION 'cannot determine partitions count for empty table';
		END IF;

		IF max_value IS NULL THEN
			RAISE EXCEPTION 'expression "%" can return NULL values', expression;
		END IF;

		p_count := 0;
		WHILE cur_value <= max_value
		LOOP
			cur_value := cur_value + p_interval;
			p_count := p_count + 1;
		END LOOP;
	END IF;

	/*
	 * In case when user doesn't want to automatically create partitions
	 * and specifies partition count as 0 then do not check boundaries
	 */
	IF p_count != 0 THEN
		/* compute right bound of partitioning through additions */
		end_value := start_value;
		FOR i IN 1..p_count
		LOOP
			end_value := end_value + p_interval;
		END LOOP;

		/* check boundaries */
		PERFORM @extschema@.check_boundaries(parent_relid,
											 expression,
											 start_value,
											 end_value);
	END IF;

	/* Create sequence for child partitions names */
	PERFORM @extschema@.create_naming_sequence(parent_relid);

	/* Insert new entry to pathman config */
	PERFORM @extschema@.add_to_pathman_config(parent_relid, expression,
											  p_interval::TEXT);

	IF p_count != 0 THEN
		part_count := @extschema@.create_range_partitions_internal(
						parent_relid,
						@extschema@.generate_range_bounds(start_value,
														  p_interval,
														  p_count),
						NULL,
						NULL);
	END IF;

	/* Relocate data if asked to */
	IF partition_data = true THEN
		PERFORM @extschema@.set_enable_parent(parent_relid, false);
		PERFORM @extschema@.partition_data(parent_relid);
	ELSE
		PERFORM @extschema@.set_enable_parent(parent_relid, true);
	END IF;

	RETURN p_count;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION @extschema@.create_range_partitions(
	parent_relid	REGCLASS,
	expression		TEXT,
	bounds			ANYARRAY,
	partition_names	TEXT[] DEFAULT NULL,
	tablespaces		TEXT[] DEFAULT NULL,
	partition_data	BOOLEAN DEFAULT TRUE)
RETURNS INTEGER AS $$
DECLARE
	part_count		INTEGER := 0;

BEGIN
	IF array_ndims(bounds) > 1 THEN
		RAISE EXCEPTION 'Bounds array must be a one dimensional array';
	END IF;

	IF array_length(bounds, 1) < 2 THEN
		RAISE EXCEPTION 'Bounds array must have at least two values';
	END IF;

	PERFORM @extschema@.prepare_for_partitioning(parent_relid,
												 expression,
												 partition_data);

	/* Check boundaries */
	PERFORM @extschema@.check_boundaries(parent_relid,
										 expression,
										 bounds[0],
										 bounds[array_length(bounds, 1) - 1]);

	/* Create sequence for child partitions names */
	PERFORM @extschema@.create_naming_sequence(parent_relid);

	/* Insert new entry to pathman config */
	PERFORM @extschema@.add_to_pathman_config(parent_relid, expression, NULL);

	/* Create partitions */
	part_count := @extschema@.create_range_partitions_internal(parent_relid,
															   bounds,
															   partition_names,
															   tablespaces);

	/* Relocate data if asked to */
	IF partition_data = true THEN
		PERFORM @extschema@.set_enable_parent(parent_relid, false);
		PERFORM @extschema@.partition_data(parent_relid);
	ELSE
		PERFORM @extschema@.set_enable_parent(parent_relid, true);
	END IF;

	RETURN part_count;
END
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION @extschema@.split_range_partition(
	partition_relid	REGCLASS,
	split_value		ANYELEMENT,
	partition_name	TEXT DEFAULT NULL,
	tablespace		TEXT DEFAULT NULL,
	OUT p_range		ANYARRAY)
RETURNS ANYARRAY AS $$
DECLARE
	parent_relid	REGCLASS;
	part_type		INTEGER;
	part_expr		TEXT;
	part_expr_type	REGTYPE;
	check_name		TEXT;
	check_cond		TEXT;
	new_partition	TEXT;

BEGIN
	parent_relid = @extschema@.get_parent_of_partition(partition_relid);

	PERFORM @extschema@.validate_relname(parent_relid);
	PERFORM @extschema@.validate_relname(partition_relid);

	/* Acquire lock on parent */
	PERFORM @extschema@.prevent_part_modification(parent_relid);

	/* Acquire data modification lock (prevent further modifications) */
	PERFORM @extschema@.prevent_data_modification(partition_relid);

	part_expr_type = @extschema@.get_partition_key_type(parent_relid);
	part_expr := @extschema@.get_partition_key(parent_relid);

	part_type := @extschema@.get_partition_type(parent_relid);

	/* Check if this is a RANGE partition */
	IF part_type != 2 THEN
		RAISE EXCEPTION '"%" is not a RANGE partition', partition_relid::TEXT;
	END IF;

	/* Get partition values range */
	EXECUTE format('SELECT @extschema@.get_part_range($1, NULL::%s)',
				   @extschema@.get_base_type(part_expr_type)::TEXT)
	USING partition_relid
	INTO p_range;

	IF p_range IS NULL THEN
		RAISE EXCEPTION 'could not find specified partition';
	END IF;

	/* Check if value fit into the range */
	IF p_range[1] > split_value OR p_range[2] <= split_value
	THEN
		RAISE EXCEPTION 'specified value does not fit into the range [%, %)',
			p_range[1], p_range[2];
	END IF;

	/* Create new partition */
	new_partition := @extschema@.create_single_range_partition(parent_relid,
															   split_value,
															   p_range[2],
															   partition_name,
															   tablespace);

	/* Copy data */
	check_cond := @extschema@.build_range_condition(new_partition::regclass,
													part_expr, split_value, p_range[2]);
	EXECUTE format('WITH part_data AS (DELETE FROM %s WHERE %s RETURNING *)
					INSERT INTO %s SELECT * FROM part_data',
				   partition_relid::TEXT,
				   check_cond,
				   new_partition);

	/* Alter original partition */
	check_cond := @extschema@.build_range_condition(partition_relid::regclass,
													part_expr, p_range[1], split_value);
	check_name := @extschema@.build_check_constraint_name(partition_relid);

	EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %s',
				   partition_relid::TEXT,
				   check_name);

	EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s)',
				   partition_relid::TEXT,
				   check_name,
				   check_cond);
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION @extschema@.append_range_partition(
	parent_relid	REGCLASS,
	partition_name	TEXT DEFAULT NULL,
	tablespace		TEXT DEFAULT NULL)
RETURNS TEXT AS $$
DECLARE
	part_expr_type	REGTYPE;
	part_name		TEXT;
	part_interval	TEXT;

BEGIN
	PERFORM @extschema@.validate_relname(parent_relid);

	/* Acquire lock on parent */
	PERFORM @extschema@.prevent_part_modification(parent_relid);

	part_expr_type := @extschema@.get_partition_key_type(parent_relid);

	IF NOT @extschema@.is_date_type(part_expr_type) AND
	   NOT @extschema@.is_operator_supported(part_expr_type, '+') THEN
		RAISE EXCEPTION 'type % does not support ''+'' operator', part_expr_type::REGTYPE;
	END IF;

	SELECT range_interval
	FROM @extschema@.pathman_config
	WHERE partrel = parent_relid
	INTO part_interval;

	EXECUTE
		format('SELECT @extschema@.append_partition_internal($1, $2, $3, ARRAY[]::%s[], $4, $5)',
			   @extschema@.get_base_type(part_expr_type)::TEXT)
	USING
		parent_relid,
		part_expr_type,
		part_interval,
		partition_name,
		tablespace
	INTO
		part_name;

	RETURN part_name;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION @extschema@.append_partition_internal(
	parent_relid	REGCLASS,
	p_atttype		REGTYPE,
	p_interval		TEXT,
	p_range			ANYARRAY DEFAULT NULL,
	partition_name	TEXT DEFAULT NULL,
	tablespace		TEXT DEFAULT NULL)
RETURNS TEXT AS $$
DECLARE
	part_expr_type	REGTYPE;
	part_name		TEXT;
	v_args_format	TEXT;

BEGIN
	IF @extschema@.get_number_of_partitions(parent_relid) = 0 THEN
		RAISE EXCEPTION 'cannot append to empty partitions set';
	END IF;

	part_expr_type := @extschema@.get_base_type(p_atttype);

	/* We have to pass fake NULL casted to column's type */
	EXECUTE format('SELECT @extschema@.get_part_range($1, -1, NULL::%s)',
				   part_expr_type::TEXT)
	USING parent_relid
	INTO p_range;

	IF p_range[2] IS NULL THEN
		RAISE EXCEPTION 'Cannot append partition because last partition''s range is half open';
	END IF;

	IF @extschema@.is_date_type(p_atttype) THEN
		v_args_format := format('$1, $2, ($2 + $3::interval)::%s, $4, $5', part_expr_type::TEXT);
	ELSE
		v_args_format := format('$1, $2, $2 + $3::%s, $4, $5', part_expr_type::TEXT);
	END IF;

	EXECUTE
		format('SELECT @extschema@.create_single_range_partition(%s)', v_args_format)
	USING
		parent_relid,
		p_range[2],
		p_interval,
		partition_name,
		tablespace
	INTO
		part_name;

	RETURN part_name;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION @extschema@.prepend_range_partition(
	parent_relid	REGCLASS,
	partition_name	TEXT DEFAULT NULL,
	tablespace		TEXT DEFAULT NULL)
RETURNS TEXT AS $$
DECLARE
	part_expr_type	REGTYPE;
	part_name		TEXT;
	part_interval	TEXT;

BEGIN
	PERFORM @extschema@.validate_relname(parent_relid);

	/* Acquire lock on parent */
	PERFORM @extschema@.prevent_part_modification(parent_relid);

	part_expr_type := @extschema@.get_partition_key_type(parent_relid);

	IF NOT @extschema@.is_date_type(part_expr_type) AND
	   NOT @extschema@.is_operator_supported(part_expr_type, '-') THEN
		RAISE EXCEPTION 'type % does not support ''-'' operator', part_expr_type::REGTYPE;
	END IF;

	SELECT range_interval
	FROM @extschema@.pathman_config
	WHERE partrel = parent_relid
	INTO part_interval;

	EXECUTE
		format('SELECT @extschema@.prepend_partition_internal($1, $2, $3, ARRAY[]::%s[], $4, $5)',
			   @extschema@.get_base_type(part_expr_type)::TEXT)
	USING
		parent_relid,
		part_expr_type,
		part_interval,
		partition_name,
		tablespace
	INTO
		part_name;

	RETURN part_name;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION @extschema@.prepend_partition_internal(
	parent_relid	REGCLASS,
	p_atttype		REGTYPE,
	p_interval		TEXT,
	p_range			ANYARRAY DEFAULT NULL,
	partition_name	TEXT DEFAULT NULL,
	tablespace		TEXT DEFAULT NULL)
RETURNS TEXT AS $$
DECLARE
	part_expr_type	REGTYPE;
	part_name		TEXT;
	v_args_format	TEXT;

BEGIN
	IF @extschema@.get_number_of_partitions(parent_relid) = 0 THEN
		RAISE EXCEPTION 'cannot prepend to empty partitions set';
	END IF;

	part_expr_type := @extschema@.get_base_type(p_atttype);

	/* We have to pass fake NULL casted to column's type */
	EXECUTE format('SELECT @extschema@.get_part_range($1, 0, NULL::%s)',
				   part_expr_type::TEXT)
	USING parent_relid
	INTO p_range;

	IF p_range[1] IS NULL THEN
		RAISE EXCEPTION 'Cannot prepend partition because first partition''s range is half open';
	END IF;

	IF @extschema@.is_date_type(p_atttype) THEN
		v_args_format := format('$1, ($2 - $3::interval)::%s, $2, $4, $5', part_expr_type::TEXT);
	ELSE
		v_args_format := format('$1, $2 - $3::%s, $2, $4, $5', part_expr_type::TEXT);
	END IF;

	EXECUTE
		format('SELECT @extschema@.create_single_range_partition(%s)', v_args_format)
	USING
		parent_relid,
		p_range[1],
		p_interval,
		partition_name,
		tablespace
	INTO
		part_name;

	RETURN part_name;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION @extschema@.add_range_partition(
	parent_relid	REGCLASS,
	start_value		ANYELEMENT,
	end_value		ANYELEMENT,
	partition_name	TEXT DEFAULT NULL,
	tablespace		TEXT DEFAULT NULL)
RETURNS TEXT AS $$
DECLARE
	part_name		TEXT;

BEGIN
	PERFORM @extschema@.validate_relname(parent_relid);

	/* Acquire lock on parent */
	PERFORM @extschema@.prevent_part_modification(parent_relid);

	IF start_value >= end_value THEN
		RAISE EXCEPTION 'failed to create partition: start_value is greater than end_value';
	END IF;

	/* check range overlap */
	IF @extschema@.get_number_of_partitions(parent_relid) > 0 THEN
		PERFORM @extschema@.check_range_available(parent_relid,
												  start_value,
												  end_value);
	END IF;

	/* Create new partition */
	part_name := @extschema@.create_single_range_partition(parent_relid,
														   start_value,
														   end_value,
														   partition_name,
														   tablespace);

	RETURN part_name;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION @extschema@.drop_range_partition(
	partition_relid	REGCLASS,
	delete_data		BOOLEAN DEFAULT TRUE)
RETURNS TEXT AS $$
DECLARE
	parent_relid	REGCLASS;
	part_name		TEXT;
	part_type		INTEGER;
	v_relkind		CHAR;
	v_rows			BIGINT;

BEGIN
	parent_relid := @extschema@.get_parent_of_partition(partition_relid);

	PERFORM @extschema@.validate_relname(parent_relid);
	PERFORM @extschema@.validate_relname(partition_relid);

	part_name := partition_relid::TEXT; /* save the name to be returned */
	part_type := @extschema@.get_partition_type(parent_relid);

	/* Check if this is a RANGE partition */
	IF part_type != 2 THEN
		RAISE EXCEPTION '"%" is not a RANGE partition', partition_relid::TEXT;
	END IF;

	/* Acquire lock on parent */
	PERFORM @extschema@.prevent_part_modification(parent_relid);

	IF NOT delete_data THEN
		EXECUTE format('INSERT INTO %s SELECT * FROM %s',
						parent_relid::TEXT,
						partition_relid::TEXT);
		GET DIAGNOSTICS v_rows = ROW_COUNT;

		/* Show number of copied rows */
		RAISE NOTICE '% rows copied from %', v_rows, partition_relid::TEXT;
	END IF;

	SELECT relkind FROM pg_catalog.pg_class
	WHERE oid = partition_relid
	INTO v_relkind;

	/*
	 * Determine the kind of child relation. It can be either regular
	 * table (r) or foreign table (f). Depending on relkind we use
	 * DROP TABLE or DROP FOREIGN TABLE.
	 */
	IF v_relkind = 'f' THEN
		EXECUTE format('DROP FOREIGN TABLE %s', partition_relid::TEXT);
	ELSE
		EXECUTE format('DROP TABLE %s', partition_relid::TEXT);
	END IF;

	RETURN part_name;
END
$$ LANGUAGE plpgsql
SET pg_pathman.enable_partitionfilter = off; /* ensures that PartitionFilter is OFF */


CREATE OR REPLACE FUNCTION @extschema@.attach_range_partition(
	parent_relid	REGCLASS,
	partition_relid	REGCLASS,
	start_value		ANYELEMENT,
	end_value		ANYELEMENT)
RETURNS TEXT AS $$
DECLARE
	part_expr			TEXT;
	rel_persistence		CHAR;
	v_init_callback		REGPROCEDURE;

BEGIN
	PERFORM @extschema@.validate_relname(parent_relid);
	PERFORM @extschema@.validate_relname(partition_relid);

	/* Acquire lock on parent */
	PERFORM @extschema@.prevent_part_modification(parent_relid);

	/* Ignore temporary tables */
	SELECT relpersistence FROM pg_catalog.pg_class
	WHERE oid = partition_relid INTO rel_persistence;

	IF rel_persistence = 't'::CHAR THEN
		RAISE EXCEPTION 'temporary table "%" cannot be used as a partition',
						partition_relid::TEXT;
	END IF;

	/* check range overlap */
	PERFORM @extschema@.check_range_available(parent_relid, start_value, end_value);

	IF NOT @extschema@.is_tuple_convertible(parent_relid, partition_relid) THEN
		RAISE EXCEPTION 'partition must have a compatible tuple format';
	END IF;

	/* Set inheritance */
	EXECUTE format('ALTER TABLE %s INHERIT %s', partition_relid, parent_relid);

	part_expr := @extschema@.get_partition_key(parent_relid);

	IF part_expr IS NULL THEN
		RAISE EXCEPTION 'table "%" is not partitioned', parent_relid::TEXT;
	END IF;

	/* Set check constraint */
	EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s)',
				   partition_relid::TEXT,
				   @extschema@.build_check_constraint_name(partition_relid),
				   @extschema@.build_range_condition(partition_relid,
													 part_expr,
													 start_value,
													 end_value));

	/* Fetch init_callback from 'params' table */
	WITH stub_callback(stub) as (values (0))
	SELECT init_callback
	FROM stub_callback
	LEFT JOIN @extschema@.pathman_config_params AS params
	ON params.partrel = parent_relid
	INTO v_init_callback;

	/* If update trigger is enabled then create one for this partition */
	if @extschema@.has_update_trigger(parent_relid) THEN
		PERFORM @extschema@.create_single_update_trigger(parent_relid, partition_relid);
	END IF;

	/* Invoke an initialization callback */
	PERFORM @extschema@.invoke_on_partition_created_callback(parent_relid,
															 partition_relid,
															 v_init_callback,
															 start_value,
															 end_value);

	RETURN partition_relid;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION @extschema@.detach_range_partition(
	partition_relid	REGCLASS)
RETURNS TEXT AS $$
DECLARE
	parent_relid	REGCLASS;
	part_type		INTEGER;

BEGIN
	parent_relid := @extschema@.get_parent_of_partition(partition_relid);

	PERFORM @extschema@.validate_relname(parent_relid);
	PERFORM @extschema@.validate_relname(partition_relid);

	/* Acquire lock on parent */
	PERFORM @extschema@.prevent_data_modification(parent_relid);

	part_type := @extschema@.get_partition_type(parent_relid);

	/* Check if this is a RANGE partition */
	IF part_type != 2 THEN
		RAISE EXCEPTION '"%" is not a RANGE partition', partition_relid::TEXT;
	END IF;

	/* Remove inheritance */
	EXECUTE format('ALTER TABLE %s NO INHERIT %s',
				   partition_relid::TEXT,
				   parent_relid::TEXT);

	/* Remove check constraint */
	EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %s',
				   partition_relid::TEXT,
				   @extschema@.build_check_constraint_name(partition_relid));

	/* Remove update trigger */
	EXECUTE format('DROP TRIGGER IF EXISTS %s ON %s',
				   @extschema@.build_update_trigger_name(parent_relid),
				   partition_relid::TEXT);

	RETURN partition_relid;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION @extschema@.create_range_partitions_internal(
	parent_relid	REGCLASS,
	bounds			ANYARRAY,
	partition_names	TEXT[],
	tablespaces		TEXT[])
RETURNS REGCLASS AS 'pg_pathman', 'create_range_partitions_internal'
LANGUAGE C;


CREATE OR REPLACE FUNCTION @extschema@.build_range_condition(
	partition_relid	REGCLASS,
	expression		TEXT,
	start_value		ANYELEMENT,
	end_value		ANYELEMENT)
RETURNS TEXT AS 'pg_pathman', 'build_range_condition'
LANGUAGE C;


CREATE OR REPLACE FUNCTION @extschema@.generate_range_bounds(
	p_start			ANYELEMENT,
	p_interval		INTERVAL,
	p_count			INTEGER)
RETURNS ANYARRAY AS 'pg_pathman', 'generate_range_bounds_pl'
LANGUAGE C STRICT;


CREATE OR REPLACE FUNCTION @extschema@.generate_range_bounds(
	p_start			ANYELEMENT,
	p_interval		ANYELEMENT,
	p_count			INTEGER)
RETURNS ANYARRAY AS 'pg_pathman', 'generate_range_bounds_pl'
LANGUAGE C STRICT;
