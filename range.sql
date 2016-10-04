/* ------------------------------------------------------------------------
 *
 * range.sql
 *      RANGE partitioning functions
 *
 * Copyright (c) 2015-2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

CREATE OR REPLACE FUNCTION @extschema@.get_sequence_name(
	plain_schema	TEXT,
	plain_relname	TEXT)
RETURNS TEXT AS
$$
BEGIN
	RETURN format('%s.%s',
				  quote_ident(plain_schema),
				  quote_ident(format('%s_seq', plain_relname)));
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION @extschema@.create_or_replace_sequence(
	plain_schema	TEXT,
	plain_relname	TEXT,
	OUT seq_name	TEXT)
AS $$
BEGIN
	seq_name := @extschema@.get_sequence_name(plain_schema, plain_relname);
	EXECUTE format('DROP SEQUENCE IF EXISTS %s', seq_name);
	EXECUTE format('CREATE SEQUENCE %s START 1', seq_name);
END
$$
LANGUAGE plpgsql;

/*
 * Check RANGE partition boundaries.
 */
CREATE OR REPLACE FUNCTION @extschema@.check_boundaries(
	parent_relid	REGCLASS,
	p_attribute		TEXT,
	p_start_value	ANYELEMENT,
	p_end_value		ANYELEMENT)
RETURNS VOID AS
$$
DECLARE
	v_min		p_start_value%TYPE;
	v_max		p_start_value%TYPE;
	v_count		BIGINT;

BEGIN
	/* Get min and max values */
	EXECUTE format('SELECT count(*), min(%1$s), max(%1$s)
					FROM %2$s WHERE NOT %1$s IS NULL',
				   p_attribute, parent_relid::TEXT)
	INTO v_count, v_min, v_max;

	/* Check if column has NULL values */
	IF v_count > 0 AND (v_min IS NULL OR v_max IS NULL) THEN
		RAISE EXCEPTION '''%'' column contains NULL values', p_attribute;
	END IF;

	/* Check lower boundary */
	IF p_start_value > v_min THEN
		RAISE EXCEPTION 'start value is less than minimum value of ''%''',
				p_attribute;
	END IF;

	/* Check upper boundary */
	IF p_end_value <= v_max THEN
		RAISE EXCEPTION 'not enough partitions to fit all values of ''%''',
				p_attribute;
	END IF;
END
$$ LANGUAGE plpgsql;

/*
 * Creates RANGE partitions for specified relation based on datetime attribute
 */
CREATE OR REPLACE FUNCTION @extschema@.create_range_partitions(
	parent_relid	REGCLASS,
	p_attribute		TEXT,
	p_start_value	ANYELEMENT,
	p_interval		INTERVAL,
	p_count			INTEGER DEFAULT NULL,
	partition_data  BOOLEAN DEFAULT TRUE)
RETURNS INTEGER AS
$$
DECLARE
	v_rows_count		INTEGER;
	v_atttype			REGTYPE;
	v_max				p_start_value%TYPE;
	v_cur_value			p_start_value%TYPE := p_start_value;
	p_end_value			p_start_value%TYPE;
	i					INTEGER;

BEGIN
	IF partition_data = true THEN
		/* Acquire data modification lock */
		PERFORM @extschema@.prevent_relation_modification(parent_relid);
	ELSE
		/* Acquire lock on parent */
		PERFORM @extschema@.lock_partitioned_relation(parent_relid);
	END IF;

	PERFORM @extschema@.validate_relname(parent_relid);
	p_attribute := lower(p_attribute);
	PERFORM @extschema@.common_relation_checks(parent_relid, p_attribute);

	IF p_count < 0 THEN
		RAISE EXCEPTION '''p_count'' must not be less than 0';
	END IF;

	/* Try to determine partitions count if not set */
	IF p_count IS NULL THEN
		EXECUTE format('SELECT count(*), max(%s) FROM %s', p_attribute, parent_relid)
		INTO v_rows_count, v_max;

		IF v_rows_count = 0 THEN
			RAISE EXCEPTION 'cannot determine partitions count for empty table';
		END IF;

		p_count := 0;
		WHILE v_cur_value <= v_max
		LOOP
			v_cur_value := v_cur_value + p_interval;
			p_count := p_count + 1;
		END LOOP;
	END IF;

	v_atttype := @extschema@.get_base_type(pg_typeof(p_start_value));

	/*
	 * In case when user doesn't want to automatically create partitions
	 * and specifies partition count as 0 then do not check boundaries
	 */
	IF p_count != 0 THEN
		/* compute right bound of partitioning through additions */
		p_end_value := p_start_value;
		FOR i IN 1..p_count
		LOOP
			p_end_value := p_end_value + p_interval;
		END LOOP;

		/* Check boundaries */
		EXECUTE format('SELECT @extschema@.check_boundaries(''%s'', ''%s'', ''%s'', ''%s''::%s)',
					   parent_relid,
					   p_attribute,
					   p_start_value,
					   p_end_value,
					   v_atttype::TEXT);
	END IF;

	/* Create sequence for child partitions names */
	PERFORM @extschema@.create_or_replace_sequence(schema, relname)
	FROM @extschema@.get_plain_schema_and_relname(parent_relid);

	/* Insert new entry to pathman config */
	INSERT INTO @extschema@.pathman_config (partrel, attname, parttype, range_interval)
	VALUES (parent_relid, p_attribute, 2, p_interval::TEXT);

	/* Create first partition */
	FOR i IN 1..p_count
	LOOP
		EXECUTE
			format('SELECT @extschema@.create_single_range_partition($1, $2, $3::%s, tablespace:=$4)',
				   v_atttype::TEXT)
		USING
			parent_relid,
			p_start_value,
			p_start_value + p_interval,
			@extschema@.get_rel_tablespace_name(parent_relid);

		p_start_value := p_start_value + p_interval;
	END LOOP;

	/* Notify backend about changes */
	PERFORM @extschema@.on_create_partitions(parent_relid);

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
 * Creates RANGE partitions for specified relation based on numerical attribute
 */
CREATE OR REPLACE FUNCTION @extschema@.create_range_partitions(
	parent_relid	REGCLASS,
	p_attribute		TEXT,
	p_start_value	ANYELEMENT,
	p_interval		ANYELEMENT,
	p_count			INTEGER DEFAULT NULL,
	partition_data  BOOLEAN DEFAULT TRUE)
RETURNS INTEGER AS
$$
DECLARE
	v_rows_count		INTEGER;
	v_max				p_start_value%TYPE;
	v_cur_value			p_start_value%TYPE := p_start_value;
	p_end_value			p_start_value%TYPE;
	i					INTEGER;

BEGIN
	IF partition_data = true THEN
		/* Acquire data modification lock */
		PERFORM @extschema@.prevent_relation_modification(parent_relid);
	ELSE
		/* Acquire lock on parent */
		PERFORM @extschema@.lock_partitioned_relation(parent_relid);
	END IF;

	PERFORM @extschema@.validate_relname(parent_relid);
	p_attribute := lower(p_attribute);
	PERFORM @extschema@.common_relation_checks(parent_relid, p_attribute);

	IF p_count < 0 THEN
		RAISE EXCEPTION 'partitions count must not be less than zero';
	END IF;

	/* Try to determine partitions count if not set */
	IF p_count IS NULL THEN
		EXECUTE format('SELECT count(*), max(%s) FROM %s', p_attribute, parent_relid)
		INTO v_rows_count, v_max;

		IF v_rows_count = 0 THEN
			RAISE EXCEPTION 'cannot determine partitions count for empty table';
		END IF;

		IF v_max IS NULL THEN
			RAISE EXCEPTION '''%'' column has NULL values', p_attribute;
		END IF;

		p_count := 0;
		WHILE v_cur_value <= v_max
		LOOP
			v_cur_value := v_cur_value + p_interval;
			p_count := p_count + 1;
		END LOOP;
	END IF;

	/*
	 * In case when user doesn't want to automatically create partitions
	 * and specifies partition count as 0 then do not check boundaries
	 */
	IF p_count != 0 THEN
		/* compute right bound of partitioning through additions */
		p_end_value := p_start_value;
		FOR i IN 1..p_count
		LOOP
			p_end_value := p_end_value + p_interval;
		END LOOP;

		/* check boundaries */
		PERFORM @extschema@.check_boundaries(parent_relid,
											 p_attribute,
											 p_start_value,
											 p_end_value);
	END IF;

	/* Create sequence for child partitions names */
	PERFORM @extschema@.create_or_replace_sequence(schema, relname)
	FROM @extschema@.get_plain_schema_and_relname(parent_relid);

	/* Insert new entry to pathman config */
	INSERT INTO @extschema@.pathman_config (partrel, attname, parttype, range_interval)
	VALUES (parent_relid, p_attribute, 2, p_interval::TEXT);

	/* create first partition */
	FOR i IN 1..p_count
	LOOP
		PERFORM @extschema@.create_single_range_partition(
			parent_relid,
			p_start_value,
			p_start_value + p_interval,
			tablespace := @extschema@.get_rel_tablespace_name(parent_relid));

		p_start_value := p_start_value + p_interval;
	END LOOP;

	/* Notify backend about changes */
	PERFORM @extschema@.on_create_partitions(parent_relid);

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
 * Creates RANGE partitions for specified range
 */
CREATE OR REPLACE FUNCTION @extschema@.create_partitions_from_range(
	parent_relid	REGCLASS,
	p_attribute		TEXT,
	p_start_value	ANYELEMENT,
	p_end_value		ANYELEMENT,
	p_interval		ANYELEMENT,
	partition_data  BOOLEAN DEFAULT TRUE)
RETURNS INTEGER AS
$$
DECLARE
	part_count		INTEGER := 0;

BEGIN
	IF partition_data = true THEN
		/* Acquire data modification lock */
		PERFORM @extschema@.prevent_relation_modification(parent_relid);
	ELSE
		/* Acquire lock on parent */
		PERFORM @extschema@.lock_partitioned_relation(parent_relid);
	END IF;

	PERFORM @extschema@.validate_relname(parent_relid);
	p_attribute := lower(p_attribute);
	PERFORM @extschema@.common_relation_checks(parent_relid, p_attribute);

	IF p_interval <= 0 THEN
		RAISE EXCEPTION 'interval must be positive';
	END IF;

	/* Check boundaries */
	PERFORM @extschema@.check_boundaries(parent_relid,
										 p_attribute,
										 p_start_value,
										 p_end_value);

	/* Create sequence for child partitions names */
	PERFORM @extschema@.create_or_replace_sequence(schema, relname)
	FROM @extschema@.get_plain_schema_and_relname(parent_relid);

	/* Insert new entry to pathman config */
	INSERT INTO @extschema@.pathman_config (partrel, attname, parttype, range_interval)
	VALUES (parent_relid, p_attribute, 2, p_interval::TEXT);

	WHILE p_start_value <= p_end_value
	LOOP
		PERFORM @extschema@.create_single_range_partition(
			parent_relid,
			p_start_value,
			p_start_value + p_interval,
			tablespace := @extschema@.get_rel_tablespace_name(parent_relid));

		p_start_value := p_start_value + p_interval;
		part_count := part_count + 1;
	END LOOP;

	/* Notify backend about changes */
	PERFORM @extschema@.on_create_partitions(parent_relid);

	/* Relocate data if asked to */
	IF partition_data = true THEN
		PERFORM @extschema@.set_enable_parent(parent_relid, false);
		PERFORM @extschema@.partition_data(parent_relid);
	ELSE
		PERFORM @extschema@.set_enable_parent(parent_relid, true);
	END IF;

	RETURN part_count; /* number of created partitions */
END
$$ LANGUAGE plpgsql;

/*
 * Creates RANGE partitions for specified range based on datetime attribute
 */
CREATE OR REPLACE FUNCTION @extschema@.create_partitions_from_range(
	parent_relid	REGCLASS,
	p_attribute		TEXT,
	p_start_value	ANYELEMENT,
	p_end_value		ANYELEMENT,
	p_interval		INTERVAL,
	partition_data  BOOLEAN DEFAULT TRUE)
RETURNS INTEGER AS
$$
DECLARE
	part_count		INTEGER := 0;

BEGIN
	IF partition_data = true THEN
		/* Acquire data modification lock */
		PERFORM @extschema@.prevent_relation_modification(parent_relid);
	ELSE
		/* Acquire lock on parent */
		PERFORM @extschema@.lock_partitioned_relation(parent_relid);
	END IF;

	PERFORM @extschema@.validate_relname(parent_relid);
	p_attribute := lower(p_attribute);
	PERFORM @extschema@.common_relation_checks(parent_relid, p_attribute);

	/* Check boundaries */
	PERFORM @extschema@.check_boundaries(parent_relid,
										 p_attribute,
										 p_start_value,
										 p_end_value);

	/* Create sequence for child partitions names */
	PERFORM @extschema@.create_or_replace_sequence(schema, relname)
	FROM @extschema@.get_plain_schema_and_relname(parent_relid);

	/* Insert new entry to pathman config */
	INSERT INTO @extschema@.pathman_config (partrel, attname, parttype, range_interval)
	VALUES (parent_relid, p_attribute, 2, p_interval::TEXT);

	WHILE p_start_value <= p_end_value
	LOOP
		EXECUTE
			format('SELECT @extschema@.create_single_range_partition($1, $2, $3::%s, tablespace:=$4);',
				   @extschema@.get_base_type(pg_typeof(p_start_value))::TEXT)
		USING
			parent_relid,
			p_start_value,
			p_start_value + p_interval,
			@extschema@.get_rel_tablespace_name(parent_relid);

		p_start_value := p_start_value + p_interval;
		part_count := part_count + 1;
	END LOOP;

	/* Notify backend about changes */
	PERFORM @extschema@.on_create_partitions(parent_relid);

	/* Relocate data if asked to */
	IF partition_data = true THEN
		PERFORM @extschema@.set_enable_parent(parent_relid, false);
		PERFORM @extschema@.partition_data(parent_relid);
	ELSE
		PERFORM @extschema@.set_enable_parent(parent_relid, true);
	END IF;

	RETURN part_count; /* number of created partitions */
END
$$ LANGUAGE plpgsql;

/*
 * Creates new RANGE partition. Returns partition name.
 * NOTE: This function SHOULD NOT take xact_handling lock (BGWs in 9.5).
 */
CREATE OR REPLACE FUNCTION @extschema@.create_single_range_partition(
	parent_relid	REGCLASS,
	p_start_value	ANYELEMENT,
	p_end_value		ANYELEMENT,
	partition_name	TEXT DEFAULT NULL,
	tablespace		TEXT DEFAULT NULL)
RETURNS TEXT AS
$$
DECLARE
	v_part_num				INT;
	v_child_relname			TEXT;
	v_plain_child_relname	TEXT;
	v_attname				TEXT;
	v_plain_schema			TEXT;
	v_plain_relname			TEXT;
	v_child_relname_exists	BOOL;
	v_seq_name				TEXT;
	v_init_callback			REGPROCEDURE;

BEGIN
	v_attname := attname FROM @extschema@.pathman_config
				 WHERE partrel = parent_relid;

	IF v_attname IS NULL THEN
		RAISE EXCEPTION 'table "%" is not partitioned', parent_relid::TEXT;
	END IF;

	SELECT * INTO v_plain_schema, v_plain_relname
	FROM @extschema@.get_plain_schema_and_relname(parent_relid);

	v_seq_name := @extschema@.get_sequence_name(v_plain_schema, v_plain_relname);

	IF partition_name IS NULL THEN
		/* Get next value from sequence */
		LOOP
			v_part_num := nextval(v_seq_name);
			v_plain_child_relname := format('%s_%s', v_plain_relname, v_part_num);
			v_child_relname := format('%s.%s',
									  quote_ident(v_plain_schema),
									  quote_ident(v_plain_child_relname));

			v_child_relname_exists := count(*) > 0
									  FROM pg_class
									  WHERE relname = v_plain_child_relname AND
											relnamespace = v_plain_schema::regnamespace
									  LIMIT 1;

			EXIT WHEN v_child_relname_exists = false;
		END LOOP;
	ELSE
		v_child_relname := partition_name;
	END IF;

	IF tablespace IS NULL THEN
		tablespace := @extschema@.get_rel_tablespace_name(parent_relid);
	END IF;

	EXECUTE format('CREATE TABLE %1$s (LIKE %2$s INCLUDING ALL)
					INHERITS (%2$s) TABLESPACE %3$s',
				   v_child_relname,
				   parent_relid::TEXT,
				   tablespace);

	EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s)',
				   v_child_relname,
				   @extschema@.build_check_constraint_name(v_child_relname::REGCLASS,
														   v_attname),
				   @extschema@.build_range_condition(v_attname,
													 p_start_value,
													 p_end_value));

	PERFORM @extschema@.copy_foreign_keys(parent_relid, v_child_relname::REGCLASS);

	/* Fetch init_callback from 'params' table */
	WITH stub_callback(stub) as (values (0))
	SELECT coalesce(init_callback, 0::REGPROCEDURE)
	FROM stub_callback
	LEFT JOIN @extschema@.pathman_config_params AS params
	ON params.partrel = parent_relid
	INTO v_init_callback;

	PERFORM @extschema@.invoke_on_partition_created_callback(parent_relid,
															 v_child_relname::REGCLASS,
															 v_init_callback,
															 p_start_value,
															 p_end_value);

	RETURN v_child_relname;
END
$$ LANGUAGE plpgsql
SET client_min_messages = WARNING;

/*
 * Split RANGE partition
 */
CREATE OR REPLACE FUNCTION @extschema@.split_range_partition(
	p_partition		REGCLASS,
	p_value			ANYELEMENT,
	partition_name	TEXT DEFAULT NULL,
	OUT p_range		ANYARRAY)
RETURNS ANYARRAY AS
$$
DECLARE
	v_parent		REGCLASS;
	v_attname		TEXT;
	v_atttype		REGTYPE;
	v_cond			TEXT;
	v_new_partition	TEXT;
	v_part_type		INTEGER;
	v_check_name	TEXT;

BEGIN
	PERFORM @extschema@.validate_relname(p_partition);
	v_parent = @extschema@.get_parent_of_partition(p_partition);

	/* Acquire lock on parent */
	PERFORM @extschema@.lock_partitioned_relation(v_parent);

	/* Acquire data modification lock (prevent further modifications) */
	PERFORM @extschema@.prevent_relation_modification(p_partition);

	SELECT attname, parttype
	FROM @extschema@.pathman_config
	WHERE partrel = v_parent
	INTO v_attname, v_part_type;

	IF v_attname IS NULL THEN
		RAISE EXCEPTION 'table "%" is not partitioned', v_parent::TEXT;
	END IF;

	/* Check if this is a RANGE partition */
	IF v_part_type != 2 THEN
		RAISE EXCEPTION 'specified partition isn''t RANGE partition';
	END IF;

	v_atttype = @extschema@.get_attribute_type(v_parent, v_attname);

	/* Get partition values range */
	EXECUTE format('SELECT @extschema@.get_part_range($1, NULL::%s)',
				   @extschema@.get_base_type(v_atttype)::TEXT)
	USING p_partition
	INTO p_range;

	IF p_range IS NULL THEN
		RAISE EXCEPTION 'could not find specified partition';
	END IF;

	/* Check if value fit into the range */
	IF p_range[1] > p_value OR p_range[2] <= p_value
	THEN
		RAISE EXCEPTION 'specified value does not fit into the range [%, %)',
			p_range[1], p_range[2];
	END IF;

	/* Create new partition */
	v_new_partition := @extschema@.create_single_range_partition(v_parent,
																 p_value,
																 p_range[2],
																 partition_name);

	/* Copy data */
	v_cond := @extschema@.build_range_condition(v_attname, p_value, p_range[2]);
	EXECUTE format('WITH part_data AS (DELETE FROM %s WHERE %s RETURNING *)
					INSERT INTO %s SELECT * FROM part_data',
				   p_partition::TEXT,
				   v_cond,
				   v_new_partition);

	/* Alter original partition */
	v_cond := @extschema@.build_range_condition(v_attname, p_range[1], p_value);
	v_check_name := @extschema@.build_check_constraint_name(p_partition, v_attname);

	EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %s',
				   p_partition::TEXT,
				   v_check_name);

	EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s)',
				   p_partition::TEXT,
				   v_check_name,
				   v_cond);

	/* Tell backend to reload configuration */
	PERFORM @extschema@.on_update_partitions(v_parent);
END
$$
LANGUAGE plpgsql;


/*
 * Merge RANGE partitions
 */
CREATE OR REPLACE FUNCTION @extschema@.merge_range_partitions(
	partition1	REGCLASS,
	partition2	REGCLASS)
RETURNS VOID AS
$$
DECLARE
	v_parent1		REGCLASS;
	v_parent2		REGCLASS;
	v_attname		TEXT;
	v_part_type		INTEGER;
	v_atttype		REGTYPE;

BEGIN
	IF partition1 = partition2 THEN
		RAISE EXCEPTION 'cannot merge partition with itself';
	END IF;

	v_parent1 := @extschema@.get_parent_of_partition(partition1);
	v_parent2 := @extschema@.get_parent_of_partition(partition2);

	/* Acquire data modification locks (prevent further modifications) */
	PERFORM @extschema@.prevent_relation_modification(partition1);
	PERFORM @extschema@.prevent_relation_modification(partition2);

	IF v_parent1 != v_parent2 THEN
		RAISE EXCEPTION 'cannot merge partitions with different parents';
	END IF;

	/* Acquire lock on parent */
	PERFORM @extschema@.lock_partitioned_relation(v_parent1);

	SELECT attname, parttype
	FROM @extschema@.pathman_config
	WHERE partrel = v_parent1
	INTO v_attname, v_part_type;

	IF v_attname IS NULL THEN
		RAISE EXCEPTION 'table "%" is not partitioned', v_parent1::TEXT;
	END IF;

	/* Check if this is a RANGE partition */
	IF v_part_type != 2 THEN
		RAISE EXCEPTION 'specified partitions aren''t RANGE partitions';
	END IF;

	v_atttype := @extschema@.get_attribute_type(partition1, v_attname);

	EXECUTE format('SELECT @extschema@.merge_range_partitions_internal($1, $2, $3, NULL::%s)',
				   @extschema@.get_base_type(v_atttype)::TEXT)
	USING v_parent1, partition1, partition2;

	/* Tell backend to reload configuration */
	PERFORM @extschema@.on_update_partitions(v_parent1);
END
$$
LANGUAGE plpgsql;


/*
 * Merge two partitions. All data will be copied to the first one. Second
 * partition will be destroyed.
 *
 * NOTE: dummy field is used to pass the element type to the function
 * (it is necessary because of pseudo-types used in function).
 */
CREATE OR REPLACE FUNCTION @extschema@.merge_range_partitions_internal(
	parent_relid	REGCLASS,
	partition1		REGCLASS,
	partition2		REGCLASS,
	dummy			ANYELEMENT,
	OUT p_range		ANYARRAY)
RETURNS ANYARRAY AS
$$
DECLARE
	v_attname		TEXT;
	v_atttype		REGTYPE;
	v_check_name	TEXT;

BEGIN
	SELECT attname FROM @extschema@.pathman_config
	WHERE partrel = parent_relid
	INTO v_attname;

	IF v_attname IS NULL THEN
		RAISE EXCEPTION 'table "%" is not partitioned', parent_relid::TEXT;
	END IF;

	v_atttype = @extschema@.get_attribute_type(parent_relid, v_attname);

	/* We have to pass fake NULL casted to column's type */
	EXECUTE format('SELECT @extschema@.get_part_range($1, NULL::%1$s) ||
						   @extschema@.get_part_range($2, NULL::%1$s)',
				   @extschema@.get_base_type(v_atttype)::TEXT)
	USING partition1, partition2
	INTO p_range;

	/* Check if ranges are adjacent */
	IF p_range[1] != p_range[4] AND p_range[2] != p_range[3] THEN
		RAISE EXCEPTION 'merge failed, partitions must be adjacent';
	END IF;

	/* Drop constraint on first partition... */
	v_check_name := @extschema@.build_check_constraint_name(partition1, v_attname);
	EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %s',
				   partition1::TEXT,
				   v_check_name);

	/* and create a new one */
	EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s)',
				   partition1::TEXT,
				   v_check_name,
				   @extschema@.build_range_condition(v_attname,
													 least(p_range[1], p_range[3]),
													 greatest(p_range[2], p_range[4])));

	/* Copy data from second partition to the first one */
	EXECUTE format('WITH part_data AS (DELETE FROM %s RETURNING *)
					INSERT INTO %s SELECT * FROM part_data',
				   partition2::TEXT,
				   partition1::TEXT);

	/* Remove second partition */
	EXECUTE format('DROP TABLE %s', partition2::TEXT);
END
$$ LANGUAGE plpgsql;


/*
 * Append new partition.
 */
CREATE OR REPLACE FUNCTION @extschema@.append_range_partition(
	parent_relid	REGCLASS,
	partition_name	TEXT DEFAULT NULL,
	tablespace		TEXT DEFAULT NULL)
RETURNS TEXT AS
$$
DECLARE
	v_attname		TEXT;
	v_atttype		REGTYPE;
	v_part_name		TEXT;
	v_interval		TEXT;

BEGIN
	/* Acquire lock on parent */
	PERFORM @extschema@.lock_partitioned_relation(parent_relid);

	SELECT attname, range_interval
	FROM @extschema@.pathman_config
	WHERE partrel = parent_relid
	INTO v_attname, v_interval;

	IF v_attname IS NULL THEN
		RAISE EXCEPTION 'table "%" is not partitioned', parent_relid::TEXT;
	END IF;

	v_atttype := @extschema@.get_attribute_type(parent_relid, v_attname);

	EXECUTE
		format('SELECT @extschema@.append_partition_internal($1, $2, $3, ARRAY[]::%s[], $4, $5)',
			   @extschema@.get_base_type(v_atttype)::TEXT)
	USING
		parent_relid,
		v_atttype,
		v_interval,
		partition_name,
		tablespace
	INTO
		v_part_name;

	/* Invalidate cache */
	PERFORM @extschema@.on_update_partitions(parent_relid);
	RETURN v_part_name;
END
$$
LANGUAGE plpgsql;

/*
 * Spawn logic for append_partition(). We have to
 * separate this in order to pass the 'p_range'.
 *
 * NOTE: we don't take a xact_handling lock here.
 */
CREATE OR REPLACE FUNCTION @extschema@.append_partition_internal(
	parent_relid	REGCLASS,
	p_atttype		REGTYPE,
	p_interval		TEXT,
	p_range			ANYARRAY DEFAULT NULL,
	partition_name	TEXT DEFAULT NULL,
	tablespace		TEXT DEFAULT NULL)
RETURNS TEXT AS
$$
DECLARE
	v_part_name		TEXT;
	v_atttype		REGTYPE;

BEGIN
	IF @extschema@.partitions_count(parent_relid) = 0 THEN
		RAISE EXCEPTION 'cannot append to empty partitions set';
	END IF;

	v_atttype := @extschema@.get_base_type(p_atttype);

	/* We have to pass fake NULL casted to column's type */
	EXECUTE format('SELECT @extschema@.get_part_range($1, -1, NULL::%s)',
				   v_atttype::TEXT)
	USING parent_relid
	INTO p_range;

	IF @extschema@.is_date_type(p_atttype) THEN
		v_part_name := @extschema@.create_single_range_partition(
			parent_relid,
			p_range[2],
			p_range[2] + p_interval::interval,
			partition_name,
			tablespace);
	ELSE
		EXECUTE
			format('SELECT @extschema@.create_single_range_partition($1, $2, $2 + $3::%s, $4, $5)',
				   v_atttype::TEXT)
		USING
			parent_relid,
			p_range[2],
			p_interval,
			partition_name,
			tablespace
		INTO
			v_part_name;
	END IF;

	RETURN v_part_name;
END
$$
LANGUAGE plpgsql;


/*
 * Prepend new partition.
 */
CREATE OR REPLACE FUNCTION @extschema@.prepend_range_partition(
	parent_relid	REGCLASS,
	partition_name	TEXT DEFAULT NULL,
	tablespace		TEXT DEFAULT NULL)
RETURNS TEXT AS
$$
DECLARE
	v_attname		TEXT;
	v_atttype		REGTYPE;
	v_part_name		TEXT;
	v_interval		TEXT;

BEGIN
	SELECT attname, range_interval
	FROM @extschema@.pathman_config
	WHERE partrel = parent_relid
	INTO v_attname, v_interval;

	IF v_attname IS NULL THEN
		RAISE EXCEPTION 'table "%" is not partitioned', parent_relid::TEXT;
	END IF;

	v_atttype := @extschema@.get_attribute_type(parent_relid, v_attname);

	EXECUTE
		format('SELECT @extschema@.prepend_partition_internal($1, $2, $3, ARRAY[]::%s[], $4, $5)',
			   @extschema@.get_base_type(v_atttype)::TEXT)
	USING
		parent_relid,
		v_atttype,
		v_interval,
		partition_name,
		tablespace
	INTO
		v_part_name;

	/* Invalidate cache */
	PERFORM @extschema@.on_update_partitions(parent_relid);
	RETURN v_part_name;
END
$$
LANGUAGE plpgsql;

/*
 * Spawn logic for prepend_partition(). We have to
 * separate this in order to pass the 'p_range'.
 *
 * NOTE: we don't take a xact_handling lock here.
 */
CREATE OR REPLACE FUNCTION @extschema@.prepend_partition_internal(
	parent_relid	REGCLASS,
	p_atttype		REGTYPE,
	p_interval		TEXT,
	p_range			ANYARRAY DEFAULT NULL,
	partition_name	TEXT DEFAULT NULL,
	tablespace		TEXT DEFAULT NULL)
RETURNS TEXT AS
$$
DECLARE
	v_part_name		TEXT;
	v_atttype		REGTYPE;

BEGIN
	IF @extschema@.partitions_count(parent_relid) = 0 THEN
		RAISE EXCEPTION 'cannot prepend to empty partitions set';
	END IF;

	v_atttype := @extschema@.get_base_type(p_atttype);

	/* We have to pass fake NULL casted to column's type */
	EXECUTE format('SELECT @extschema@.get_part_range($1, 0, NULL::%s)',
				   v_atttype::TEXT)
	USING parent_relid
	INTO p_range;

	IF @extschema@.is_date_type(p_atttype) THEN
		v_part_name := @extschema@.create_single_range_partition(
			parent_relid,
			p_range[1] - p_interval::interval,
			p_range[1],
			partition_name,
			tablespace);
	ELSE
		EXECUTE
			format('SELECT @extschema@.create_single_range_partition($1, $2 - $3::%s, $2, $4, $5)',
				   v_atttype::TEXT)
		USING
			parent_relid,
			p_range[1],
			p_interval,
			partition_name,
			tablespace
		INTO
			v_part_name;
	END IF;

	RETURN v_part_name;
END
$$
LANGUAGE plpgsql;


/*
 * Add new partition
 */
CREATE OR REPLACE FUNCTION @extschema@.add_range_partition(
	parent_relid	REGCLASS,
	p_start_value	ANYELEMENT,
	p_end_value		ANYELEMENT,
	partition_name	TEXT DEFAULT NULL,
	tablespace		TEXT DEFAULT NULL)
RETURNS TEXT AS
$$
DECLARE
	v_part_name		TEXT;

BEGIN
	/* Acquire lock on parent */
	PERFORM @extschema@.lock_partitioned_relation(parent_relid);

	IF p_start_value >= p_end_value THEN
		RAISE EXCEPTION 'failed to create partition: p_start_value is greater than p_end_value';
	END IF;

	/* check range overlap */
	IF @extschema@.partitions_count(parent_relid) > 0
	   AND @extschema@.check_overlap(parent_relid, p_start_value, p_end_value) THEN
		RAISE EXCEPTION 'specified range overlaps with existing partitions';
	END IF;

	/* Create new partition */
	v_part_name := @extschema@.create_single_range_partition(parent_relid,
															 p_start_value,
															 p_end_value,
															 partition_name,
															 tablespace);
	PERFORM @extschema@.on_update_partitions(parent_relid);

	RETURN v_part_name;
END
$$
LANGUAGE plpgsql;


/*
 * Drop range partition
 */
CREATE OR REPLACE FUNCTION @extschema@.drop_range_partition(
	p_partition		REGCLASS)
RETURNS TEXT AS
$$
DECLARE
	parent_relid	REGCLASS;
	part_name		TEXT;

BEGIN
	parent_relid := @extschema@.get_parent_of_partition(p_partition);
	part_name := p_partition::TEXT; /* save the name to be returned */

	/* Acquire lock on parent */
	PERFORM @extschema@.lock_partitioned_relation(parent_relid);

	/* Drop table */
	EXECUTE format('DROP TABLE %s', part_name);

	/* Invalidate cache */
	PERFORM @extschema@.on_update_partitions(parent_relid);

	RETURN part_name;
END
$$
LANGUAGE plpgsql;


/*
 * Attach range partition
 */
CREATE OR REPLACE FUNCTION @extschema@.attach_range_partition(
	parent_relid	REGCLASS,
	p_partition		REGCLASS,
	p_start_value	ANYELEMENT,
	p_end_value		ANYELEMENT)
RETURNS TEXT AS
$$
DECLARE
	v_attname			TEXT;
	rel_persistence		CHAR;

BEGIN
	/* Acquire lock on parent */
	PERFORM @extschema@.lock_partitioned_relation(parent_relid);

	/* Ignore temporary tables */
	SELECT relpersistence FROM pg_catalog.pg_class
	WHERE oid = p_partition INTO rel_persistence;

	IF rel_persistence = 't'::CHAR THEN
		RAISE EXCEPTION 'temporary table "%" cannot be used as a partition',
						p_partition::TEXT;
	END IF;

	IF @extschema@.check_overlap(parent_relid, p_start_value, p_end_value) THEN
		RAISE EXCEPTION 'specified range overlaps with existing partitions';
	END IF;

	IF NOT @extschema@.validate_relations_equality(parent_relid, p_partition) THEN
		RAISE EXCEPTION 'partition must have the exact same structure as parent';
	END IF;

	/* Set inheritance */
	EXECUTE format('ALTER TABLE %s INHERIT %s', p_partition, parent_relid);

	v_attname := attname FROM @extschema@.pathman_config WHERE partrel = parent_relid;

	IF v_attname IS NULL THEN
		RAISE EXCEPTION 'table "%" is not partitioned', parent_relid::TEXT;
	END IF;

	/* Set check constraint */
	EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s)',
				   p_partition::TEXT,
				   @extschema@.build_check_constraint_name(p_partition, v_attname),
				   @extschema@.build_range_condition(v_attname,
													 p_start_value,
													 p_end_value));

	/* Invalidate cache */
	PERFORM @extschema@.on_update_partitions(parent_relid);

	RETURN p_partition;
END
$$
LANGUAGE plpgsql;


/*
 * Detach range partition
 */
CREATE OR REPLACE FUNCTION @extschema@.detach_range_partition(
	p_partition		REGCLASS)
RETURNS TEXT AS
$$
DECLARE
	v_attname		TEXT;
	parent_relid	REGCLASS;

BEGIN
	parent_relid := @extschema@.get_parent_of_partition(p_partition);

	/* Acquire lock on parent */
	PERFORM @extschema@.lock_partitioned_relation(parent_relid);

	v_attname := attname
	FROM @extschema@.pathman_config
	WHERE partrel = parent_relid;

	IF v_attname IS NULL THEN
		RAISE EXCEPTION 'table "%" is not partitioned', parent_relid::TEXT;
	END IF;

	/* Remove inheritance */
	EXECUTE format('ALTER TABLE %s NO INHERIT %s',
				   p_partition::TEXT,
				   parent_relid::TEXT);

	/* Remove check constraint */
	EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %s',
				   p_partition::TEXT,
				   @extschema@.build_check_constraint_name(p_partition, v_attname));

	/* Invalidate cache */
	PERFORM @extschema@.on_update_partitions(parent_relid);

	RETURN p_partition;
END
$$
LANGUAGE plpgsql;


/*
 * Creates an update trigger
 */
CREATE OR REPLACE FUNCTION @extschema@.create_range_update_trigger(
	IN parent_relid		REGCLASS)
RETURNS TEXT AS
$$
DECLARE
	func			TEXT := 'CREATE OR REPLACE FUNCTION %1$s()
							 RETURNS TRIGGER AS
							 $body$
							 DECLARE
								old_oid		Oid;
								new_oid		Oid;

							 BEGIN
								old_oid := TG_RELID;
								new_oid := @extschema@.find_or_create_range_partition(
												''%2$s''::regclass, NEW.%3$s);

								IF old_oid = new_oid THEN
									RETURN NEW;
								END IF;

								EXECUTE format(''DELETE FROM %%s WHERE %5$s'',
											   old_oid::regclass::text)
								USING %6$s;

								EXECUTE format(''INSERT INTO %%s VALUES (%7$s)'',
											   new_oid::regclass::text)
								USING %8$s;

								RETURN NULL;
							 END $body$
							 LANGUAGE plpgsql';

	trigger			TEXT := 'CREATE TRIGGER %s ' ||
							'BEFORE UPDATE ON %s ' ||
							'FOR EACH ROW EXECUTE PROCEDURE %s()';

	triggername		TEXT;
	funcname		TEXT;
	att_names		TEXT;
	old_fields		TEXT;
	new_fields		TEXT;
	att_val_fmt		TEXT;
	att_fmt			TEXT;
	attr			TEXT;
	rec				RECORD;

BEGIN
	attr := attname FROM @extschema@.pathman_config WHERE partrel = parent_relid;

	IF attr IS NULL THEN
		RAISE EXCEPTION 'table "%" is not partitioned', parent_relid::TEXT;
	END IF;

	SELECT string_agg(attname, ', '),
		   string_agg('OLD.' || attname, ', '),
		   string_agg('NEW.' || attname, ', '),
		   string_agg('CASE WHEN NOT $' || attnum || ' IS NULL THEN ' ||
							attname || ' = $' || attnum || ' ' ||
					  'ELSE ' ||
							attname || ' IS NULL END',
					  ' AND '),
		   string_agg('$' || attnum, ', ')
	FROM pg_attribute
	WHERE attrelid::REGCLASS = parent_relid AND attnum > 0
	INTO att_names,
		 old_fields,
		 new_fields,
		 att_val_fmt,
		 att_fmt;

	/* Build trigger & trigger function's names */
	funcname := @extschema@.build_update_trigger_func_name(parent_relid);
	triggername := @extschema@.build_update_trigger_name(parent_relid);

	/* Create function for trigger */
	EXECUTE format(func, funcname, parent_relid, attr, 0, att_val_fmt,
				   old_fields, att_fmt, new_fields);

	/* Create trigger on every partition */
	FOR rec in (SELECT * FROM pg_catalog.pg_inherits
				WHERE inhparent = parent_relid)
	LOOP
		EXECUTE format(trigger,
					   triggername,
					   rec.inhrelid::REGCLASS::TEXT,
					   funcname);
	END LOOP;

	RETURN funcname;
END
$$ LANGUAGE plpgsql;

/*
 * Construct CHECK constraint condition for a range partition.
 */
CREATE OR REPLACE FUNCTION @extschema@.build_range_condition(
	p_attname		TEXT,
	p_start_value	ANYELEMENT,
	p_end_value		ANYELEMENT)
RETURNS TEXT AS 'pg_pathman', 'build_range_condition'
LANGUAGE C;

/*
 * Returns N-th range (as an array of two elements).
 */
CREATE OR REPLACE FUNCTION @extschema@.get_part_range(
	parent_relid	REGCLASS,
	partition_idx	INTEGER,
	dummy			ANYELEMENT)
RETURNS ANYARRAY AS 'pg_pathman', 'get_part_range_by_idx'
LANGUAGE C;

/*
 * Returns min and max values for specified RANGE partition.
 */
CREATE OR REPLACE FUNCTION @extschema@.get_part_range(
	partition_relid	REGCLASS,
	dummy			ANYELEMENT)
RETURNS ANYARRAY AS 'pg_pathman', 'get_part_range_by_oid'
LANGUAGE C;

/*
 * Checks if range overlaps with existing partitions.
 * Returns TRUE if overlaps and FALSE otherwise.
 */
CREATE OR REPLACE FUNCTION @extschema@.check_overlap(
	parent_relid	REGCLASS,
	range_min		ANYELEMENT,
	range_max		ANYELEMENT)
RETURNS BOOLEAN AS 'pg_pathman', 'check_overlap'
LANGUAGE C;

/*
 * Needed for an UPDATE trigger.
 */
CREATE OR REPLACE FUNCTION @extschema@.find_or_create_range_partition(
	parent_relid	REGCLASS,
	value			ANYELEMENT)
RETURNS REGCLASS AS 'pg_pathman', 'find_or_create_range_partition'
LANGUAGE C;
