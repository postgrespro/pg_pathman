ALTER TABLE @extschema@.pathman_config DROP CONSTRAINT pathman_config_interval_check;

DROP FUNCTION @extschema@.validate_interval_value(REGCLASS, TEXT, INTEGER,
	TEXT, TEXT);
CREATE OR REPLACE FUNCTION @extschema@.validate_interval_value(
	partrel			REGCLASS,
	expr			TEXT,
	parttype		INTEGER,
	range_interval	TEXT)
RETURNS BOOL AS 'pg_pathman', 'validate_interval_value'
LANGUAGE C;

ALTER TABLE @extschema@.pathman_config DROP COLUMN cooked_expr;
ALTER TABLE @extschema@.pathman_config ADD CONSTRAINT pathman_config_interval_check
	CHECK (@extschema@.validate_interval_value(partrel,
											   expr,
											   parttype,
											   range_interval));

/*
 * Get parsed and analyzed expression.
 */
CREATE OR REPLACE FUNCTION @extschema@.get_partition_cooked_key(
	parent_relid	REGCLASS)
RETURNS TEXT AS 'pg_pathman', 'get_partition_cooked_key_pl'
LANGUAGE C STRICT;

/*
 * Add new partition
 */
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

	/* Acquire lock on parent's scheme */
	PERFORM @extschema@.prevent_part_modification(parent_relid);

	IF start_value >= end_value THEN
		RAISE EXCEPTION 'failed to create partition: start_value is greater than end_value';
	END IF;

	/* Check range overlap */
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

/*
 * Append new partition.
 */
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

	/* Acquire lock on parent's scheme */
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

/*
 * Attach range partition
 */
CREATE OR REPLACE FUNCTION @extschema@.attach_range_partition(
	parent_relid	REGCLASS,
	partition_relid	REGCLASS,
	start_value		ANYELEMENT,
	end_value		ANYELEMENT)
RETURNS TEXT AS $$
DECLARE
	part_expr			TEXT;
	part_type			INTEGER;
	rel_persistence		CHAR;
	v_init_callback		REGPROCEDURE;

BEGIN
	PERFORM @extschema@.validate_relname(parent_relid);
	PERFORM @extschema@.validate_relname(partition_relid);

	/* Acquire lock on parent's scheme */
	PERFORM @extschema@.prevent_part_modification(parent_relid);

	/* Ignore temporary tables */
	SELECT relpersistence FROM pg_catalog.pg_class
	WHERE oid = partition_relid INTO rel_persistence;

	IF rel_persistence = 't'::CHAR THEN
		RAISE EXCEPTION 'temporary table "%" cannot be used as a partition',
						partition_relid::TEXT;
	END IF;

	/* Check range overlap */
	PERFORM @extschema@.check_range_available(parent_relid, start_value, end_value);

	IF NOT @extschema@.is_tuple_convertible(parent_relid, partition_relid) THEN
		RAISE EXCEPTION 'partition must have a compatible tuple format';
	END IF;

	part_expr := @extschema@.get_partition_key(parent_relid);
	part_type := @extschema@.get_partition_type(parent_relid);

	IF part_expr IS NULL THEN
		RAISE EXCEPTION 'table "%" is not partitioned', parent_relid::TEXT;
	END IF;

	/* Check if this is a RANGE partition */
	IF part_type != 2 THEN
		RAISE EXCEPTION '"%" is not a RANGE partition', partition_relid::TEXT;
	END IF;

	/* Set inheritance */
	EXECUTE format('ALTER TABLE %s INHERIT %s', partition_relid, parent_relid);

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

	/* Invoke an initialization callback */
	PERFORM @extschema@.invoke_on_partition_created_callback(parent_relid,
															 partition_relid,
															 v_init_callback,
															 start_value,
															 end_value);

	RETURN partition_relid;
END
$$ LANGUAGE plpgsql;

/*
 * Create a naming sequence for partitioned table.
 */
CREATE OR REPLACE FUNCTION @extschema@.create_naming_sequence(
	parent_relid	REGCLASS)
RETURNS TEXT AS $$
DECLARE
	seq_name TEXT;

BEGIN
	seq_name := @extschema@.build_sequence_name(parent_relid);

	EXECUTE format('DROP SEQUENCE IF EXISTS %s', seq_name);
	EXECUTE format('CREATE SEQUENCE %s START 1', seq_name);

	RETURN seq_name;
END
$$ LANGUAGE plpgsql
SET client_min_messages = WARNING; /* mute NOTICE message */

/*
 * Creates RANGE partitions for specified relation based on datetime attribute
 */
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

	/*
	 * In case when user doesn't want to automatically create partitions
	 * and specifies partition count as 0 then do not check boundaries
	 */
	IF p_count != 0 THEN
		/* Compute right bound of partitioning through additions */
		end_value := start_value;
		FOR i IN 1..p_count
		LOOP
			end_value := end_value + p_interval;
		END LOOP;

		/* Check boundaries */
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

	RETURN part_count;
END
$$ LANGUAGE plpgsql;

/*
 * Creates RANGE partitions for specified relation based on numerical expression
 */
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
		/* Compute right bound of partitioning through additions */
		end_value := start_value;
		FOR i IN 1..p_count
		LOOP
			end_value := end_value + p_interval;
		END LOOP;

		/* Check boundaries */
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

/*
 * Creates RANGE partitions for specified relation based on bounds array
 */
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
										 bounds[1],
										 bounds[array_length(bounds, 1)]);

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

/*
 * Detach range partition
 */
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

	/* Acquire lock on partition's scheme */
	PERFORM @extschema@.prevent_part_modification(partition_relid);

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

	RETURN partition_relid;
END
$$ LANGUAGE plpgsql;

/*
 * Disable pathman partitioning for specified relation.
 */
CREATE OR REPLACE FUNCTION @extschema@.disable_pathman_for(
	parent_relid	REGCLASS)
RETURNS VOID AS $$
BEGIN
	PERFORM @extschema@.validate_relname(parent_relid);

	/* Delete rows from both config tables */
	DELETE FROM @extschema@.pathman_config WHERE partrel = parent_relid;
	DELETE FROM @extschema@.pathman_config_params WHERE partrel = parent_relid;
END
$$ LANGUAGE plpgsql STRICT;

/*
 * Drop a naming sequence for partitioned table.
 */
CREATE OR REPLACE FUNCTION @extschema@.drop_naming_sequence(
	parent_relid	REGCLASS)
RETURNS VOID AS $$
DECLARE
	seq_name TEXT;

BEGIN
	seq_name := @extschema@.build_sequence_name(parent_relid);

	EXECUTE format('DROP SEQUENCE IF EXISTS %s', seq_name);
END
$$ LANGUAGE plpgsql
SET client_min_messages = WARNING; /* mute NOTICE message */

/*
 * Drop partitions. If delete_data set to TRUE, partitions
 * will be dropped with all the data.
 */
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

/*
 * Drop range partition
 */
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

	/* Acquire lock on parent's scheme */
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

CREATE FUNCTION @extschema@.pathman_version()
RETURNS CSTRING AS 'pg_pathman', 'pathman_version'
LANGUAGE C STRICT;

/*
 * Get number of partitions managed by pg_pathman.
 */
CREATE OR REPLACE FUNCTION @extschema@.get_number_of_partitions(
	parent_relid	REGCLASS)
RETURNS INT4 AS
$$
	SELECT count(*)::INT4
	FROM pg_catalog.pg_inherits
	WHERE inhparent = parent_relid;
$$
LANGUAGE sql STRICT;

/*
 * Get partitioning key.
 */
DROP FUNCTION @extschema@.get_partition_key(REGCLASS);
CREATE FUNCTION @extschema@.get_partition_key(
	parent_relid	REGCLASS)
RETURNS TEXT AS
$$
	SELECT expr
	FROM @extschema@.pathman_config
	WHERE partrel = parent_relid;
$$
LANGUAGE sql STRICT;

/*
 * Get partitioning key type.
 */
DROP FUNCTION @extschema@.get_partition_key_type(REGCLASS);
CREATE FUNCTION @extschema@.get_partition_key_type(
	parent_relid	REGCLASS)
RETURNS REGTYPE AS 'pg_pathman', 'get_partition_key_type_pl'
LANGUAGE C STRICT;

/*
 * Get partitioning type.
 */
DROP FUNCTION @extschema@.get_partition_type(REGCLASS);
CREATE OR REPLACE FUNCTION @extschema@.get_partition_type(
	parent_relid	REGCLASS)
RETURNS INT4 AS
$$
	SELECT parttype
	FROM @extschema@.pathman_config
	WHERE partrel = parent_relid;
$$
LANGUAGE sql STRICT;

/*
 * Merge RANGE partitions.
 */
DROP FUNCTION @extschema@.merge_range_partitions(regclass[]);
DROP FUNCTION @extschema@.merge_range_partitions(regclass, regclass);

CREATE OR REPLACE FUNCTION @extschema@.merge_range_partitions(
	variadic partitions		REGCLASS[])
RETURNS REGCLASS AS 'pg_pathman', 'merge_range_partitions'
LANGUAGE C STRICT;

/*
 * Prepend new partition.
 */
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

	/* Acquire lock on parent's scheme */
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

/*
 * Show all existing concurrent partitioning tasks.
 */
DROP VIEW @extschema@.pathman_concurrent_part_tasks;
DROP FUNCTION @extschema@.show_concurrent_part_tasks();
CREATE FUNCTION @extschema@.show_concurrent_part_tasks()
RETURNS TABLE (
	userid		REGROLE,
	pid			INT,
	dbid		OID,
	relid		REGCLASS,
	processed	INT8,
	status		TEXT)
AS 'pg_pathman', 'show_concurrent_part_tasks_internal'
LANGUAGE C STRICT;

CREATE VIEW @extschema@.pathman_concurrent_part_tasks
AS SELECT * FROM @extschema@.show_concurrent_part_tasks();
GRANT SELECT ON @extschema@.pathman_concurrent_part_tasks TO PUBLIC;

/*
 * Split RANGE partition in two using a pivot.
 */
DROP FUNCTION @extschema@.split_range_partition(regclass, anyelement, text, text, OUT anyarray);
CREATE OR REPLACE FUNCTION @extschema@.split_range_partition(
	partition_relid	REGCLASS,
	split_value		ANYELEMENT,
	partition_name	TEXT DEFAULT NULL,
	tablespace		TEXT DEFAULT NULL)
RETURNS REGCLASS AS 'pg_pathman', 'split_range_partition'
LANGUAGE C;

DROP FUNCTION @extschema@.build_update_trigger_func_name(regclass);
DROP FUNCTION @extschema@.build_update_trigger_name(regclass);
DROP FUNCTION @extschema@.create_single_update_trigger(regclass, regclass);
DROP FUNCTION @extschema@.create_update_triggers(regclass);
DROP FUNCTION @extschema@.drop_triggers(regclass);
DROP FUNCTION @extschema@.has_update_trigger(regclass);
DROP FUNCTION @extschema@.pathman_update_trigger_func() CASCADE;
DROP FUNCTION @extschema@.get_pathman_lib_version();
