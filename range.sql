/* ------------------------------------------------------------------------
 *
 * range.sql
 *      RANGE partitioning functions
 *
 * Copyright (c) 2015-2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

CREATE OR REPLACE FUNCTION @extschema@.get_sequence_name(plain_schema TEXT, plain_relname TEXT)
RETURNS TEXT AS
$$
BEGIN
	RETURN format('%s.%s', plain_schema, quote_ident(format('%s_seq', plain_relname)));
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION @extschema@.create_or_replace_sequence(plain_schema TEXT, plain_relname TEXT, OUT seq_name TEXT)
AS $$
DECLARE
BEGIN
	seq_name := @extschema@.get_sequence_name(plain_schema, plain_relname);
	EXECUTE format('DROP SEQUENCE IF EXISTS %s', seq_name);
	EXECUTE format('CREATE SEQUENCE %s START 1', seq_name);
END
$$
LANGUAGE plpgsql;

/*
 * Creates RANGE partitions for specified relation based on datetime attribute
 */
CREATE OR REPLACE FUNCTION @extschema@.create_range_partitions(
	p_relation      REGCLASS
	, p_attribute   TEXT
	, p_start_value ANYELEMENT
	, p_interval    INTERVAL
	, p_count       INTEGER DEFAULT NULL)
RETURNS INTEGER AS
$$
DECLARE
	v_relname       TEXT;
	v_rows_count    INTEGER;
	v_max           p_start_value%TYPE;
	v_cur_value     p_start_value%TYPE := p_start_value;
	v_plain_relname TEXT;
	v_plain_schema  TEXT;
	i               INTEGER;
BEGIN
	v_relname := @extschema@.validate_relname(p_relation);
	p_attribute := lower(p_attribute);
	PERFORM @extschema@.common_relation_checks(v_relname, p_attribute);

	/* Try to determine partitions count if not set */
	IF p_count IS NULL THEN
		EXECUTE format('SELECT count(*), max(%s) FROM %s'
					   , p_attribute, p_relation)
		INTO v_rows_count, v_max;

		IF v_rows_count = 0 THEN
			RAISE EXCEPTION 'Cannot determine partitions count for empty table';
		END IF;

		p_count := 0;
		WHILE v_cur_value <= v_max
		LOOP
			v_cur_value := v_cur_value + p_interval;
			p_count := p_count + 1;
		END LOOP;
	END IF;

	/* Check boundaries */
	EXECUTE format('SELECT @extschema@.check_boundaries(''%s'', ''%s'', ''%s'', ''%s''::%s)'
				   , v_relname
				   , p_attribute
				   , p_start_value
				   , p_start_value + p_interval*p_count
				   , pg_typeof(p_start_value));

	/* Create sequence for child partitions names */
	SELECT * INTO v_plain_schema, v_plain_relname FROM @extschema@.get_plain_schema_and_relname(p_relation);
	PERFORM @extschema@.create_or_replace_sequence(v_plain_schema, v_plain_relname);

	/* Insert new entry to pathman config */
	INSERT INTO @extschema@.pathman_config (relname, attname, parttype, range_interval)
	VALUES (v_relname, p_attribute, 2, p_interval::text);

	/* create first partition */
	FOR i IN 1..p_count
	LOOP
		EXECUTE format('SELECT @extschema@.create_single_range_partition($1, $2, $3::%s);', pg_typeof(p_start_value))
		USING v_relname, p_start_value, p_start_value + p_interval;

		p_start_value := p_start_value + p_interval;
	END LOOP;

	/* Create triggers */
	PERFORM @extschema@.create_range_insert_trigger(v_relname, p_attribute);
	-- PERFORM create_hash_update_trigger(relation, attribute, partitions_count);
	/* Notify backend about changes */
	PERFORM @extschema@.on_create_partitions(p_relation::oid);

	/* Copy data */
	PERFORM @extschema@.partition_data(p_relation);

	RETURN p_count;

EXCEPTION WHEN others THEN
	RAISE EXCEPTION '%', SQLERRM;
END
$$ LANGUAGE plpgsql;

/*
 * Creates RANGE partitions for specified relation based on numerical attribute
 */
CREATE OR REPLACE FUNCTION @extschema@.create_range_partitions(
	p_relation      REGCLASS
	, p_attribute   TEXT
	, p_start_value ANYELEMENT
	, p_interval    ANYELEMENT
	, p_count       INTEGER DEFAULT NULL)
RETURNS INTEGER AS
$$
DECLARE
	v_relname       TEXT;
	v_rows_count    INTEGER;
	v_max           p_start_value%TYPE;
	v_cur_value     p_start_value%TYPE := p_start_value;
	i               INTEGER;
	v_plain_schema  TEXT;
	v_plain_relname TEXT;
BEGIN
	v_relname := @extschema@.validate_relname(p_relation);
	p_attribute := lower(p_attribute);
	PERFORM @extschema@.common_relation_checks(p_relation, p_attribute);

	IF p_count <= 0 THEN
		RAISE EXCEPTION 'Partitions count must be greater than zero';
	END IF;

	/* Try to determine partitions count if not set */
	IF p_count IS NULL THEN
		EXECUTE format('SELECT count(*), max(%s) FROM %s'
					   , p_attribute, p_relation)
		INTO v_rows_count, v_max;

		IF v_rows_count = 0 THEN
			RAISE EXCEPTION 'Cannot determine partitions count for empty table';
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

	/* check boundaries */
	PERFORM @extschema@.check_boundaries(p_relation
										 , p_attribute
										 , p_start_value
										 , p_start_value + p_interval*p_count);

	/* Create sequence for child partitions names */
	SELECT * INTO v_plain_schema, v_plain_relname FROM @extschema@.get_plain_schema_and_relname(p_relation);
	PERFORM @extschema@.create_or_replace_sequence(v_plain_schema, v_plain_relname);

	/* Insert new entry to pathman config */
	INSERT INTO @extschema@.pathman_config (relname, attname, parttype, range_interval)
	VALUES (v_relname, p_attribute, 2, p_interval::text);

	/* create first partition */
	FOR i IN 1..p_count
	LOOP
		PERFORM @extschema@.create_single_range_partition(p_relation
														  , p_start_value
														  , p_start_value + p_interval);
		p_start_value := p_start_value + p_interval;
	END LOOP;

	/* Create triggers */
	PERFORM @extschema@.create_range_insert_trigger(p_relation, p_attribute);
	-- PERFORM create_hash_update_trigger(relation, attribute, partitions_count);

	/* Notify backend about changes */
	PERFORM @extschema@.on_create_partitions(p_relation::regclass::oid);

	/* Copy data */
	PERFORM @extschema@.partition_data(p_relation);

	RETURN p_count;

EXCEPTION WHEN others THEN
	RAISE EXCEPTION '%', SQLERRM;
END
$$ LANGUAGE plpgsql;

/*
 * Creates RANGE partitions for specified range
 */
CREATE OR REPLACE FUNCTION @extschema@.create_partitions_from_range(
	p_relation      REGCLASS
	, p_attribute   TEXT
	, p_start_value ANYELEMENT
	, p_end_value   ANYELEMENT
	, p_interval    ANYELEMENT)
RETURNS INTEGER AS
$$
DECLARE
	v_relname       TEXT;
	v_plain_schema  TEXT;
	v_plain_relname TEXT;
	i               INTEGER := 0;
BEGIN
	v_relname := @extschema@.validate_relname(p_relation);
	p_attribute := lower(p_attribute);
	PERFORM @extschema@.common_relation_checks(p_relation, p_attribute);

	IF p_interval <= 0 THEN
		RAISE EXCEPTION 'Interval must be positive';
	END IF;

	/* Create sequence for child partitions names */
	SELECT * INTO v_plain_schema, v_plain_relname FROM @extschema@.get_plain_schema_and_relname(p_relation);
	PERFORM @extschema@.create_or_replace_sequence(v_plain_schema, v_plain_relname);

	/* check boundaries */
	PERFORM @extschema@.check_boundaries(p_relation
										 , p_attribute
										 , p_start_value
										 , p_end_value);

	/* Insert new entry to pathman config */
	INSERT INTO @extschema@.pathman_config (relname, attname, parttype, range_interval)
	VALUES (v_relname, p_attribute, 2, p_interval::text);

	WHILE p_start_value <= p_end_value
	LOOP
		PERFORM @extschema@.create_single_range_partition(p_relation
														 , p_start_value
														 , p_start_value + p_interval);
		p_start_value := p_start_value + p_interval;
		i := i + 1;
	END LOOP;

	/* Create triggers */
	PERFORM @extschema@.create_range_insert_trigger(p_relation, p_attribute);

	/* Notify backend about changes */
	PERFORM @extschema@.on_create_partitions(p_relation::regclass::oid);

	/* Copy data */
	PERFORM @extschema@.partition_data(p_relation);

	RETURN i;

EXCEPTION WHEN others THEN
	RAISE EXCEPTION '%', SQLERRM;
END
$$ LANGUAGE plpgsql;

/*
 * Creates RANGE partitions for specified range based on datetime attribute
 */
CREATE OR REPLACE FUNCTION @extschema@.create_partitions_from_range(
	p_relation      REGCLASS
	, p_attribute   TEXT
	, p_start_value ANYELEMENT
	, p_end_value   ANYELEMENT
	, p_interval    INTERVAL)
RETURNS INTEGER AS
$$
DECLARE
	v_relname       TEXT;
	v_plain_schema  TEXT;
	v_plain_relname TEXT;
	i               INTEGER := 0;
BEGIN
	v_relname := @extschema@.validate_relname(p_relation);
	p_attribute := lower(p_attribute);
	PERFORM @extschema@.common_relation_checks(p_relation, p_attribute);

	/* Create sequence for child partitions names */
	SELECT * INTO v_plain_schema, v_plain_relname FROM @extschema@.get_plain_schema_and_relname(p_relation);
	PERFORM @extschema@.create_or_replace_sequence(v_plain_schema, v_plain_relname);

	/* check boundaries */
	PERFORM @extschema@.check_boundaries(p_relation
										 , p_attribute
										 , p_start_value
										 , p_end_value);

	/* Insert new entry to pathman config */
	INSERT INTO @extschema@.pathman_config (relname, attname, parttype, range_interval)
	VALUES (v_relname, p_attribute, 2, p_interval::text);

	WHILE p_start_value <= p_end_value
	LOOP
		EXECUTE format('SELECT @extschema@.create_single_range_partition($1, $2, $3::%s);', pg_typeof(p_start_value))
		USING p_relation, p_start_value, p_start_value + p_interval;
		p_start_value := p_start_value + p_interval;
		i := i + 1;
	END LOOP;

	/* Create triggers */
	PERFORM @extschema@.create_range_insert_trigger(p_relation, p_attribute);

	/* Notify backend about changes */
	PERFORM @extschema@.on_create_partitions(p_relation::regclass::oid);

	/* Copy data */
	PERFORM @extschema@.partition_data(p_relation);

	RETURN i;

EXCEPTION WHEN others THEN
	RAISE EXCEPTION '%', SQLERRM;
END
$$ LANGUAGE plpgsql;

/*
 *
 */
CREATE OR REPLACE FUNCTION @extschema@.check_boundaries(
	p_relation REGCLASS
	, p_attribute TEXT
	, p_start_value ANYELEMENT
	, p_end_value ANYELEMENT)
RETURNS VOID AS
$$
DECLARE
	v_min p_start_value%TYPE;
	v_max p_start_value%TYPE;
	v_count INTEGER;
BEGIN
	/* Get min and max values */
	EXECUTE format('SELECT count(*), min(%s), max(%s) FROM %s WHERE NOT %s IS NULL',
				   p_attribute, p_attribute, p_relation::text, p_attribute)
	INTO v_count, v_min, v_max;

	/* check if column has NULL values */
	IF v_count > 0 AND (v_min IS NULL OR v_max IS NULL) THEN
		RAISE EXCEPTION '''%'' column has NULL values', p_attribute;
	END IF;

	/* check lower boundary */
	IF p_start_value > v_min THEN
		RAISE EXCEPTION 'Start value is less than minimum value of ''%'''
			, p_attribute;
	END IF;

	/* check upper boundary */
	IF p_end_value <= v_max  THEN
		RAISE EXCEPTION 'Not enough partitions to fit all the values of ''%'''
			, p_attribute;
	END IF;
END
$$ LANGUAGE plpgsql;

/*
 * Formats range condition. Utility function.
 */
CREATE OR REPLACE FUNCTION @extschema@.get_range_condition(
	p_attname TEXT
	, p_start_value ANYELEMENT
	, p_end_value ANYELEMENT)
RETURNS TEXT AS
$$
DECLARE
	v_type REGTYPE;
	v_sql  TEXT;
BEGIN
	/* determine the type of values */
	v_type := pg_typeof(p_start_value);

	/* we cannot use placeholders in DDL queries, so we are using format(...) */
	IF v_type IN ('date'::regtype, 'timestamp'::regtype, 'timestamptz'::regtype) THEN
		v_sql := '%s >= ''%s'' AND %s < ''%s''';
	ELSE
		v_sql := '%s >= %s AND %s < %s';
	END IF;

	v_sql := format(v_sql
					, p_attname
					, p_start_value
					, p_attname
					, p_end_value);
	RETURN v_sql;
END
$$
LANGUAGE plpgsql;

/*
 * Creates new RANGE partition. Returns partition name
 */
CREATE OR REPLACE FUNCTION @extschema@.create_single_range_partition(
	p_parent REGCLASS
	, p_start_value  ANYELEMENT
	, p_end_value    ANYELEMENT)
RETURNS TEXT AS
$$
DECLARE
	v_part_num      INT;
	v_child_relname TEXT;
	v_plain_child_relname TEXT;
	v_attname       TEXT;
	v_sql           TEXT;
	v_cond          TEXT;
	v_plain_schema  TEXT;
	v_plain_relname TEXT;
	v_child_relname_exists INTEGER := 1;
	v_seq_name      TEXT;
BEGIN
	v_attname := attname FROM @extschema@.pathman_config
				 WHERE relname::regclass = p_parent;

	IF v_attname IS NULL THEN
		RAISE EXCEPTION 'Table % is not partitioned', quote_ident(p_parent::TEXT);
	END IF;

	SELECT * INTO v_plain_schema, v_plain_relname
	FROM @extschema@.get_plain_schema_and_relname(p_parent);

	v_seq_name := @extschema@.get_sequence_name(v_plain_schema, v_plain_relname);

	/* get next value from sequence */
	LOOP
		v_part_num := nextval(v_seq_name);
		v_plain_child_relname := format('%s_%s', v_plain_relname, v_part_num);
		v_child_relname := format('%s.%s',
								  v_plain_schema,
								  quote_ident(v_plain_child_relname));
		v_child_relname_exists := count(*)
								  FROM pg_class
								  WHERE relnamespace::regnamespace || '.' || relname = v_child_relname
								  LIMIT 1;
		EXIT WHEN v_child_relname_exists = 0;
	END LOOP;

	EXECUTE format('CREATE TABLE %s (LIKE %s INCLUDING ALL)'
				   , v_child_relname
				   , p_parent);

	EXECUTE format('ALTER TABLE %s INHERIT %s'
				   , v_child_relname
				   , p_parent);

	v_cond := @extschema@.get_range_condition(v_attname, p_start_value, p_end_value);
	v_sql := format('ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s)'
					, v_child_relname
					, quote_ident(format('%s_%s_check', v_plain_schema, v_plain_child_relname))
					, v_cond);

	EXECUTE v_sql;
	RETURN v_child_relname;
END
$$ LANGUAGE plpgsql;

/*
 * Split RANGE partition
 */
CREATE OR REPLACE FUNCTION @extschema@.split_range_partition(
	p_partition REGCLASS
	, p_value ANYELEMENT
	, OUT p_range ANYARRAY)
RETURNS ANYARRAY AS
$$
DECLARE
	v_parent_relid  OID;
	v_child_relid   OID := p_partition::oid;
	v_attname       TEXT;
	v_cond          TEXT;
	v_new_partition TEXT;
	v_part_type     INTEGER;
	v_part_relname  TEXT;
	v_plain_schema  TEXT;
	v_plain_relname TEXT;
	v_check_name    TEXT;
BEGIN
	v_part_relname := @extschema@.validate_relname(p_partition);

	v_parent_relid := inhparent
					  FROM pg_inherits
					  WHERE inhrelid = v_child_relid;

	SELECT attname, parttype
	FROM @extschema@.pathman_config
	WHERE relname::regclass = v_parent_relid::regclass
	INTO v_attname, v_part_type;

	IF v_attname IS NULL THEN
		RAISE EXCEPTION 'Table % is not partitioned',
						quote_ident(v_parent_relid::regclass::text);
	END IF;

	SELECT * INTO v_plain_schema, v_plain_relname
	FROM @extschema@.get_plain_schema_and_relname(p_partition);

	/* Check if this is RANGE partition */
	IF v_part_type != 2 THEN
		RAISE EXCEPTION 'Specified partition isn''t RANGE partition';
	END IF;

	/* Get partition values range */
	p_range := @extschema@.get_partition_range(v_parent_relid, v_child_relid, 0);
	IF p_range IS NULL THEN
		RAISE EXCEPTION 'Could not find specified partition';
	END IF;

	/* Check if value fit into the range */
	IF p_range[1] > p_value OR p_range[2] <= p_value
	THEN
		RAISE EXCEPTION 'Specified value does not fit into the range [%, %)',
			p_range[1], p_range[2];
	END IF;

	/* Create new partition */
	RAISE NOTICE 'Creating new partition...';
	v_new_partition := @extschema@.create_single_range_partition(
							@extschema@.get_schema_qualified_name(v_parent_relid::regclass, '.'),
							p_value,
							p_range[2]);

	/* Copy data */
	RAISE NOTICE 'Copying data to new partition...';
	v_cond := @extschema@.get_range_condition(v_attname, p_value, p_range[2]);
	EXECUTE format('
				WITH part_data AS (
					DELETE FROM %s WHERE %s RETURNING *)
				INSERT INTO %s SELECT * FROM part_data'
				, p_partition
				, v_cond
				, v_new_partition);

	/* Alter original partition */
	RAISE NOTICE 'Altering original partition...';
	v_cond := @extschema@.get_range_condition(v_attname, p_range[1], p_value);
	v_check_name := quote_ident(format('%s_%s_check', v_plain_schema, v_plain_relname));
	EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %s'
				   , p_partition::text
				   , v_check_name);
	EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s)'
				   , p_partition
				   , v_check_name
				   , v_cond);

	/* Tell backend to reload configuration */
	PERFORM @extschema@.on_update_partitions(v_parent_relid::oid);

	RAISE NOTICE 'Done!';
END
$$
LANGUAGE plpgsql;


/*
 * Merge RANGE partitions
 */
CREATE OR REPLACE FUNCTION @extschema@.merge_range_partitions(
	p_partition1 REGCLASS
	, p_partition2 REGCLASS)
RETURNS VOID AS
$$
DECLARE
	v_parent_relid1 OID;
	v_parent_relid2 OID;
	v_part1_relid OID := p_partition1::oid;
	v_part2_relid OID := p_partition2::oid;
	v_part1_relname TEXT;
	v_part2_relname TEXT;
	v_attname TEXT;
	v_part_type INTEGER;
	v_atttype TEXT;
BEGIN
	v_part1_relname := @extschema@.validate_relname(p_partition1);
	v_part2_relname := @extschema@.validate_relname(p_partition2);

	IF v_part1_relid = v_part2_relid THEN
		RAISE EXCEPTION 'Cannot merge partition to itself';
	END IF;

	v_parent_relid1 := inhparent FROM pg_inherits WHERE inhrelid = v_part1_relid;
	v_parent_relid2 := inhparent FROM pg_inherits WHERE inhrelid = v_part2_relid;

	IF v_parent_relid1 != v_parent_relid2 THEN
		RAISE EXCEPTION 'Cannot merge partitions having different parents';
	END IF;

	SELECT attname, parttype
	FROM @extschema@.pathman_config
	WHERE relname::regclass = v_parent_relid1::regclass
	INTO v_attname, v_part_type;

	IF v_attname IS NULL THEN
		RAISE EXCEPTION 'Table % is not partitioned',
						quote_ident(v_parent_relid1::regclass::text);
	END IF;

	/* Check if this is RANGE partition */
	IF v_part_type != 2 THEN
		RAISE EXCEPTION 'Specified partitions aren''t RANGE partitions';
	END IF;

	v_atttype := @extschema@.get_attribute_type_name(p_partition1, v_attname);

	EXECUTE format('SELECT @extschema@.merge_range_partitions_internal($1, $2 , $3, NULL::%s)', v_atttype)
	USING v_parent_relid1, p_partition1 , p_partition2;

	/* Tell backend to reload configuration */
	PERFORM @extschema@.on_update_partitions(v_parent_relid1::oid);

	RAISE NOTICE 'Done!';
END
$$
LANGUAGE plpgsql;


/*
 * Merge two partitions. All data will be copied to the first one. Second
 * partition will be destroyed.
 *
 * Notes: dummy field is used to pass the element type to the function
 * (it is necessary because of pseudo-types used in function)
 */
CREATE OR REPLACE FUNCTION @extschema@.merge_range_partitions_internal(
	p_parent_relid OID
	, p_part1 REGCLASS
	, p_part2 REGCLASS
	, dummy ANYELEMENT
	, OUT p_range ANYARRAY)
RETURNS ANYARRAY AS
$$
DECLARE
	v_attname       TEXT;
	v_cond          TEXT;
	v_plain_schema  TEXT;
	v_plain_relname TEXT;
	v_child_relname TEXT;
	v_check_name    TEXT;
BEGIN
	SELECT attname FROM @extschema@.pathman_config
	WHERE relname::regclass = p_parent_relid::regclass
	INTO v_attname;

	IF v_attname IS NULL THEN
		RAISE EXCEPTION 'Table % is not partitioned',
						quote_ident(p_parent_relid::regclass::text);
	END IF;

	SELECT * INTO v_plain_schema, v_plain_relname
	FROM @extschema@.get_plain_schema_and_relname(p_part1);

	/*
	 * Get ranges
	 * first and second elements of array are MIN and MAX of partition1
	 * third and forth elements are MIN and MAX of partition2
	 */
	p_range := @extschema@.get_partition_range(p_parent_relid, p_part1::oid, 0) ||
			   @extschema@.get_partition_range(p_parent_relid, p_part2::oid, 0);

	/* Check if ranges are adjacent */
	IF p_range[1] != p_range[4] AND p_range[2] != p_range[3] THEN
		RAISE EXCEPTION 'Merge failed. Partitions must be adjacent';
	END IF;

	/* Extend first partition */
	v_cond := @extschema@.get_range_condition(v_attname
											  , least(p_range[1], p_range[3])
											  , greatest(p_range[2], p_range[4]));

	/* Alter first partition */
	RAISE NOTICE 'Altering first partition...';
	v_check_name := quote_ident(v_plain_schema || '_' || v_plain_relname || '_check');
	EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %s'
				   , p_part1::text
				   , v_check_name);
	EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s)'
				   , p_part1::text
				   , v_check_name
				   , v_cond);

	/* Copy data from second partition to the first one */
	RAISE NOTICE 'Copying data...';
	EXECUTE format('WITH part_data AS (DELETE FROM %s RETURNING *)
					INSERT INTO %s SELECT * FROM part_data'
				   , p_part2::text
				   , p_part1::text);

	/* Remove second partition */
	RAISE NOTICE 'Dropping second partition...';
	EXECUTE format('DROP TABLE %s', p_part2::text);
END
$$ LANGUAGE plpgsql;


/*
 * Append new partition
 */
CREATE OR REPLACE FUNCTION @extschema@.append_range_partition(
	p_relation REGCLASS)
RETURNS TEXT AS
$$
DECLARE
	v_attname	TEXT;
	v_atttype	TEXT;
	v_part_name	TEXT;
	v_interval	TEXT;
BEGIN
	/* Prevent concurrent partition creation */
	PERFORM @extschema@.acquire_partitions_lock();

	SELECT attname, range_interval
	FROM @extschema@.pathman_config
	WHERE relname::regclass = p_relation
	INTO v_attname, v_interval;

	IF v_attname IS NULL THEN
		RAISE EXCEPTION 'Table % is not partitioned', quote_ident(p_relation::TEXT);
	END IF;

	v_atttype := @extschema@.get_attribute_type_name(p_relation, v_attname);

	EXECUTE format('SELECT @extschema@.append_partition_internal($1, $2, $3, ARRAY[]::%s[])', v_atttype)
	INTO v_part_name
	USING p_relation, v_atttype, v_interval;

	/* Invalidate cache */
	PERFORM @extschema@.on_update_partitions(p_relation::oid);

	/* Release lock */
	PERFORM @extschema@.release_partitions_lock();

	RAISE NOTICE 'Done!';
	RETURN v_part_name;

EXCEPTION WHEN others THEN
	RAISE EXCEPTION '%', SQLERRM;
END
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION @extschema@.append_partition_internal(
	p_relation REGCLASS
	, p_atttype TEXT
	, p_interval TEXT
	, p_range ANYARRAY DEFAULT NULL)
RETURNS TEXT AS
$$
DECLARE
	v_part_name TEXT;
BEGIN
	p_range := @extschema@.get_range_by_idx(p_relation::oid, -1, 0);
	RAISE NOTICE 'Appending new partition...';
	IF @extschema@.is_date(p_atttype::regtype) THEN
		v_part_name := @extschema@.create_single_range_partition(p_relation
																 , p_range[2]
																 , p_range[2] + p_interval::interval);
	ELSE
		EXECUTE format('SELECT @extschema@.create_single_range_partition($1, $2, $2 + $3::%s)', p_atttype)
		USING p_relation, p_range[2], p_interval
		INTO v_part_name;
	END IF;

	RETURN v_part_name;
END
$$
LANGUAGE plpgsql;


/*
 * Prepend new partition
 */
CREATE OR REPLACE FUNCTION @extschema@.prepend_range_partition(p_relation REGCLASS)
RETURNS TEXT AS
$$
DECLARE
	v_attname TEXT;
	v_atttype TEXT;
	v_part_name TEXT;
	v_interval TEXT;
BEGIN
	/* Prevent concurrent partition creation */
	PERFORM @extschema@.acquire_partitions_lock();

	SELECT attname, range_interval
	FROM @extschema@.pathman_config
	WHERE relname::regclass = p_relation
	INTO v_attname, v_interval;

	IF v_attname IS NULL THEN
		RAISE EXCEPTION 'Table % is not partitioned', quote_ident(p_relation::TEXT);
	END IF;

	v_atttype := @extschema@.get_attribute_type_name(p_relation, v_attname);

	EXECUTE format('SELECT @extschema@.prepend_partition_internal($1, $2, $3, ARRAY[]::%s[])', v_atttype)
	INTO v_part_name
	USING p_relation, v_atttype, v_interval;

	/* Invalidate cache */
	PERFORM @extschema@.on_update_partitions(p_relation::oid);

	/* Release lock */
	PERFORM @extschema@.release_partitions_lock();

	RAISE NOTICE 'Done!';
	RETURN v_part_name;

EXCEPTION WHEN others THEN
	RAISE EXCEPTION '%', SQLERRM;
END
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION @extschema@.prepend_partition_internal(
	p_relation REGCLASS
	, p_atttype TEXT
	, p_interval TEXT
	, p_range ANYARRAY DEFAULT NULL)
RETURNS TEXT AS
$$
DECLARE
	v_part_name TEXT;
BEGIN
	p_range := @extschema@.get_range_by_idx(p_relation::oid, 0, 0);
	RAISE NOTICE 'Prepending new partition...';

	IF @extschema@.is_date(p_atttype::regtype) THEN
		v_part_name := @extschema@.create_single_range_partition(p_relation
																 , p_range[1] - p_interval::interval
																 , p_range[1]);
	ELSE
		EXECUTE format('SELECT @extschema@.create_single_range_partition($1, $2 - $3::%s, $2)', p_atttype)
		USING p_relation, p_range[1], p_interval
		INTO v_part_name;
	END IF;

	RETURN v_part_name;
END
$$
LANGUAGE plpgsql;


/*
 * Add new partition
 */
CREATE OR REPLACE FUNCTION @extschema@.add_range_partition(
	p_relation REGCLASS
	, p_start_value ANYELEMENT
	, p_end_value ANYELEMENT)
RETURNS TEXT AS
$$
DECLARE
	v_part_name TEXT;
BEGIN
	/* Prevent concurrent partition creation */
	PERFORM @extschema@.acquire_partitions_lock();

	/* check range overlap */
	IF @extschema@.check_overlap(p_relation::oid, p_start_value, p_end_value) != FALSE THEN
		RAISE EXCEPTION 'Specified range overlaps with existing partitions';
	END IF;

	IF p_start_value >= p_end_value THEN
		RAISE EXCEPTION 'Failed to create partition: p_start_value is greater than p_end_value';
	END IF;

	/* Create new partition */
	v_part_name := @extschema@.create_single_range_partition(p_relation, p_start_value, p_end_value);
	PERFORM @extschema@.on_update_partitions(p_relation::oid);

	/* Release lock */
	PERFORM @extschema@.release_partitions_lock();

	RAISE NOTICE 'Done!';
	RETURN v_part_name;

EXCEPTION WHEN others THEN
	RAISE EXCEPTION '%', SQLERRM;
END
$$
LANGUAGE plpgsql;


/*
 * Drop range partition
 */
CREATE OR REPLACE FUNCTION @extschema@.drop_range_partition(
	p_partition REGCLASS)
RETURNS TEXT AS
$$
DECLARE
	v_part_name TEXT := p_partition::TEXT;
	v_parent 	TEXT;
	v_count     INTEGER;
BEGIN
	/* Prevent concurrent partition management */
	PERFORM @extschema@.acquire_partitions_lock();

	/* Parent table name */
	SELECT inhparent::regclass INTO v_parent
	FROM pg_inherits WHERE inhrelid::regclass = p_partition;

	IF v_parent IS NULL THEN
		RAISE EXCEPTION 'Partition ''%'' not found', p_partition;
	END IF;

	/* Drop table and update cache */
	EXECUTE format('DROP TABLE %s', p_partition::TEXT);
	PERFORM @extschema@.on_update_partitions(v_parent::regclass::oid);

	/* Release lock */
	PERFORM @extschema@.release_partitions_lock();

	RETURN v_part_name;

EXCEPTION WHEN others THEN
	RAISE EXCEPTION '%', SQLERRM;
END
$$
LANGUAGE plpgsql;


/*
 * Attach range partition
 */
CREATE OR REPLACE FUNCTION @extschema@.attach_range_partition(
	p_relation       REGCLASS
	, p_partition    REGCLASS
	, p_start_value  ANYELEMENT
	, p_end_value    ANYELEMENT)
RETURNS TEXT AS
$$
DECLARE
	v_attname        TEXT;
	v_cond           TEXT;
	v_plain_partname TEXT;
	v_plain_schema   TEXT;
	rel_persistence  CHAR;
BEGIN
	/* Ignore temporary tables */
	SELECT relpersistence FROM pg_catalog.pg_class WHERE oid = p_partition INTO rel_persistence;
	IF rel_persistence = 't'::CHAR THEN
		RAISE EXCEPTION 'Temporary table % cannot be used as a partition',
			quote_ident(p_partition::TEXT);
	END IF;

	/* Prevent concurrent partition management */
	PERFORM @extschema@.acquire_partitions_lock();

	IF @extschema@.check_overlap(p_relation::oid, p_start_value, p_end_value) != FALSE THEN
		RAISE EXCEPTION 'Specified range overlaps with existing partitions';
	END IF;

	IF NOT @extschema@.validate_relations_equality(p_relation, p_partition) THEN
		RAISE EXCEPTION 'Partition must have the exact same structure as parent';
	END IF;

	/* Set inheritance */
	EXECUTE format('ALTER TABLE %s INHERIT %s'
				   , p_partition
				   , p_relation);

	/* Set check constraint */
	v_attname := attname
	FROM @extschema@.pathman_config
	WHERE relname::regclass = p_relation;

	IF v_attname IS NULL THEN
		RAISE EXCEPTION 'Table % is not partitioned', quote_ident(p_relation::TEXT);
	END IF;

	v_cond := @extschema@.get_range_condition(v_attname, p_start_value, p_end_value);

	/* Plain partition name and schema */
	SELECT * INTO v_plain_schema, v_plain_partname FROM @extschema@.get_plain_schema_and_relname(p_partition);

	EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s)'
				   , p_partition
				   , v_plain_schema || '_' || quote_ident(v_plain_partname || '_check')
				   , v_cond);

	/* Invalidate cache */
	PERFORM @extschema@.on_update_partitions(p_relation::oid);

	/* Release lock */
	PERFORM @extschema@.release_partitions_lock();
	RETURN p_partition;

EXCEPTION WHEN others THEN
	RAISE EXCEPTION '%', SQLERRM;
END
$$
LANGUAGE plpgsql;


/*
 * Detach range partition
 */
CREATE OR REPLACE FUNCTION @extschema@.detach_range_partition(
	p_partition TEXT)
RETURNS TEXT AS
$$
DECLARE
	v_parent TEXT;
BEGIN
	/* Prevent concurrent partition management */
	PERFORM @extschema@.acquire_partitions_lock();

	/* Parent table */
	SELECT inhparent::regclass INTO v_parent
	FROM pg_inherits WHERE inhrelid = p_partition::regclass::oid;

	/* Remove inheritance */
	EXECUTE format('ALTER TABLE %s NO INHERIT %s'
				   , p_partition
				   , v_parent);

	/* Remove check constraint */
	EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %s_check'
				   , p_partition
				   , @extschema@.get_schema_qualified_name(p_partition::regclass));

	/* Invalidate cache */
	PERFORM @extschema@.on_update_partitions(v_parent::regclass::oid);

	/* Release lock */
	PERFORM @extschema@.release_partitions_lock();
	RETURN p_partition;

EXCEPTION WHEN others THEN
	RAISE EXCEPTION '%', SQLERRM;
END
$$
LANGUAGE plpgsql;


/*
 * Creates range partitioning insert trigger
 */
CREATE OR REPLACE FUNCTION @extschema@.create_range_insert_trigger(
	v_relation    REGCLASS
	, v_attname   TEXT)
RETURNS VOID AS
$$
DECLARE
	v_func TEXT := '
		CREATE OR REPLACE FUNCTION %s()
		RETURNS TRIGGER
		AS $body$
		DECLARE
			v_part_relid OID;
		BEGIN
			IF TG_OP = ''INSERT'' THEN
				IF NEW.%2$s IS NULL THEN
					RAISE EXCEPTION ''ERROR: NULL value in partitioning key'';
				END IF;
				v_part_relid := @extschema@.find_or_create_range_partition(TG_RELID, NEW.%2$s);
				IF NOT v_part_relid IS NULL THEN
					EXECUTE format(''INSERT INTO %%s SELECT $1.*'', v_part_relid::regclass)
					USING NEW;
				ELSE
					RAISE EXCEPTION ''ERROR: Cannot find partition'';
				END IF;
			END IF;
			RETURN NULL;
		END
		$body$ LANGUAGE plpgsql;';
	v_funcname TEXT;
	v_trigger TEXT := '
		CREATE TRIGGER %s
		BEFORE INSERT ON %s
		FOR EACH ROW EXECUTE PROCEDURE %s();';
	v_triggername   TEXT;
	v_plain_relname TEXT;
	v_plain_schema  TEXT;
BEGIN
	SELECT * INTO v_plain_schema, v_plain_relname
	FROM @extschema@.get_plain_schema_and_relname(v_relation);

	v_funcname := format(quote_ident('%s_insert_trigger_func'), v_plain_relname);
	v_triggername := format('"%s_%s_insert_trigger"', v_plain_schema, v_plain_relname);

	v_func := format(v_func, v_funcname, v_attname);
	v_trigger := format(v_trigger, v_triggername, v_relation, v_funcname);

	EXECUTE v_func;
	EXECUTE v_trigger;
	RETURN;
END
$$ LANGUAGE plpgsql;


/*
 * Creates an update trigger
 */
CREATE OR REPLACE FUNCTION @extschema@.create_range_update_trigger(
	IN relation REGCLASS)
RETURNS TEXT AS
$$
DECLARE
	func TEXT := '
		CREATE OR REPLACE FUNCTION %s_update_trigger_func()
		RETURNS TRIGGER AS
		$body$
		DECLARE
			old_oid INTEGER;
			new_oid INTEGER;
			q TEXT;
		BEGIN
			old_oid := TG_RELID;
			new_oid := @extschema@.find_or_create_range_partition(''%1$s''::regclass::oid, NEW.%2$s);
			IF old_oid = new_oid THEN RETURN NEW; END IF;
			q := format(''DELETE FROM %%s WHERE %4$s'', old_oid::regclass::text);
			EXECUTE q USING %5$s;
			q := format(''INSERT INTO %%s VALUES (%6$s)'', new_oid::regclass::text);
			EXECUTE q USING %7$s;
			RETURN NULL;
		END $body$ LANGUAGE plpgsql';
	trigger TEXT := 'CREATE TRIGGER %s_update_trigger ' ||
		'BEFORE UPDATE ON %s ' ||
		'FOR EACH ROW EXECUTE PROCEDURE %s_update_trigger_func()';
	att_names   TEXT;
	old_fields  TEXT;
	new_fields  TEXT;
	att_val_fmt TEXT;
	att_fmt     TEXT;
	relid       INTEGER;
	rec         RECORD;
	num         INTEGER := 0;
	attr        TEXT;
BEGIN
	relid := relation::oid;
	SELECT string_agg(attname, ', '),
		   string_agg('OLD.' || attname, ', '),
		   string_agg('NEW.' || attname, ', '),
		   string_agg('CASE WHEN NOT $' || attnum || ' IS NULL THEN ' || attname || ' = $' || attnum ||
					  ' ELSE ' || attname || ' IS NULL END', ' AND '),
		   string_agg('$' || attnum, ', ')
	FROM pg_attribute
	WHERE attrelid=relid AND attnum>0
	INTO   att_names,
		   old_fields,
		   new_fields,
		   att_val_fmt,
		   att_fmt;

	attr := attname
	FROM @extschema@.pathman_config
	WHERE relname::regclass = relation;

	IF attr IS NULL THEN
		RAISE EXCEPTION 'Table % is not partitioned', quote_ident(relation::TEXT);
	END IF;

	EXECUTE format(func, relation, attr, 0, att_val_fmt,
				   old_fields, att_fmt, new_fields);
	FOR rec in (SELECT * FROM pg_inherits WHERE inhparent = relation::regclass::oid)
	LOOP
		EXECUTE format(trigger
					   , @extschema@.get_schema_qualified_name(relation::regclass)
					   , rec.inhrelid::regclass
					   , relation);
		num := num + 1;
	END LOOP;

	RETURN format('%s_update_trigger_func()', relation);
END
$$ LANGUAGE plpgsql;


/*
 * Drop partitions
 * If delete_data set to TRUE then partitions will be dropped with all the data
 */
CREATE OR REPLACE FUNCTION @extschema@.drop_range_partitions(
	relation REGCLASS
	, delete_data BOOLEAN DEFAULT FALSE)
RETURNS INTEGER AS
$$
DECLARE
	v_rec        RECORD;
	v_rows       INTEGER;
	v_part_count INTEGER := 0;
	v_relname    TEXT;
BEGIN
	v_relname := @extschema@.validate_relname(relation);

	/* Drop trigger first */
	PERFORM @extschema@.drop_range_triggers(relation);

	FOR v_rec IN (SELECT inhrelid::regclass::text AS tbl
				  FROM pg_inherits WHERE inhparent::regclass = relation)
	LOOP
		IF NOT delete_data THEN
			EXECUTE format('WITH part_data AS (DELETE FROM %s RETURNING *)
							INSERT INTO %s SELECT * FROM part_data'
						   , v_rec.tbl
						   , relation::text);
			GET DIAGNOSTICS v_rows = ROW_COUNT;
			RAISE NOTICE '% rows copied from %', v_rows, v_rec.tbl;
		END IF;
		EXECUTE format('DROP TABLE %s', v_rec.tbl);
		v_part_count := v_part_count + 1;
	END LOOP;

	DELETE FROM @extschema@.pathman_config WHERE relname::regclass = relation;

	/* Notify backend about changes */
	PERFORM @extschema@.on_remove_partitions(relation::oid);

	RETURN v_part_count;
END
$$ LANGUAGE plpgsql;


/*
 * Drop trigger
 */
CREATE OR REPLACE FUNCTION @extschema@.drop_range_triggers(IN relation REGCLASS)
RETURNS VOID AS
$$
DECLARE
	schema  TEXT;
	relname TEXT;
BEGIN
	SELECT * INTO schema, relname
	FROM @extschema@.get_plain_schema_and_relname(relation);

	EXECUTE format('DROP TRIGGER IF EXISTS %s ON %s CASCADE'
				   , format('"%s_%s_insert_trigger"', schema, relname)
				   , relation::TEXT);
END
$$ LANGUAGE plpgsql;


/*
 * Internal function used to create new partitions on insert or update trigger.
 * Invoked from C-function find_or_create_range_partition().
 */
CREATE OR REPLACE FUNCTION @extschema@.append_partitions_on_demand_internal(
	p_relid OID
	, p_new_value ANYELEMENT)
RETURNS OID AS
$$
DECLARE
	v_cnt INTEGER := 0;
	i INTEGER := 0;
	v_part TEXT;
	v_interval TEXT;
	v_attname TEXT;
	v_min p_new_value%TYPE;
	v_max p_new_value%TYPE;
	v_cur_value p_new_value%TYPE;
	v_next_value p_new_value%TYPE;
	v_is_date BOOLEAN;
BEGIN
	/* get attribute name and interval */
	SELECT attname, range_interval
	FROM @extschema@.pathman_config
	WHERE relname::regclass = p_relid::regclass
	INTO v_attname, v_interval;

	IF v_attname IS NULL THEN
		RAISE EXCEPTION 'Table % is not partitioned',
						quote_ident(p_relid::regclass::text);
	END IF;

	v_min := @extschema@.get_min_range_value(p_relid::regclass::oid, p_new_value);
	v_max := @extschema@.get_max_range_value(p_relid::regclass::oid, p_new_value);

	v_is_date := @extschema@.is_date(pg_typeof(p_new_value)::regtype);

	IF p_new_value >= v_max THEN
		v_cur_value := v_max;
		WHILE v_cur_value <= p_new_value AND i < 1000
		LOOP
			IF v_is_date THEN
				v_next_value := v_cur_value + v_interval::interval;
			ELSE
				EXECUTE format('SELECT $1 + $2::%s', pg_typeof(p_new_value))
				USING v_cur_value, v_interval
				INTO v_next_value;
			END IF;

			v_part := @extschema@.create_single_range_partition(
							@extschema@.get_schema_qualified_name(p_relid::regclass, '.')
							, v_cur_value
							, v_next_value);
			i := i + 1;
			v_cur_value := v_next_value;
			RAISE NOTICE 'partition % created', v_part;
		END LOOP;
	ELSIF p_new_value <= v_min THEN
		v_cur_value := v_min;
		WHILE v_cur_value >= p_new_value AND i < 1000
		LOOP
			IF v_is_date THEN
				v_next_value := v_cur_value - v_interval::interval;
			ELSE
				EXECUTE format('SELECT $1 - $2::%s', pg_typeof(p_new_value))
				USING v_cur_value, v_interval
				INTO v_next_value;
			END IF;

			v_part := @extschema@.create_single_range_partition(
							@extschema@.get_schema_qualified_name(p_relid::regclass, '.')
							, v_next_value
							, v_cur_value);
			i := i + 1;
			v_cur_value := v_next_value;
			RAISE NOTICE 'partition % created', v_part;
		END LOOP;
	ELSE
		RAISE NOTICE 'Not implemented yet';
	END IF;

	IF i > 0 THEN
		RETURN v_part::regclass::oid;
	END IF;
	RETURN NULL;
END
$$ LANGUAGE plpgsql;
