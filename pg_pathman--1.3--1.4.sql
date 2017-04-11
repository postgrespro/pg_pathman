/* ------------------------------------------------------------------------
 *
 * pg_pathman--1.3--1.4.sql
 *		Migration scripts to version 1.4
 *
 * Copyright (c) 2015-2017, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

DROP FUNCTION @extschema@.validate_interval_value(REGCLASS, TEXT, INTEGER, TEXT) CASCADE;
CREATE OR REPLACE FUNCTION @extschema@.validate_interval_value(
	atttype			OID,
	parttype		INTEGER,
	range_interval	TEXT)
RETURNS BOOL AS 'pg_pathman', 'validate_interval_value'
LANGUAGE C;

DROP FUNCTION @extschema@.is_attribute_nullable(REGCLASS, TEXT);

ALTER TABLE @extschema@.pathman_config ADD COLUMN expression_p TEXT DEFAULT '--not set--';
ALTER TABLE @extschema@.pathman_config ADD COLUMN atttype OID DEFAULT 1;
ALTER TABLE @extschema@.pathman_config ADD COLUMN upd_expr BOOL DEFAULT FALSE;

/* update constraint */
ALTER TABLE @extschema@.pathman_config
	ADD CONSTRAINT pathman_config_interval_check CHECK (@extschema@.validate_interval_value(atttype,
										 parttype,
										 range_interval));

/* mark 'expression_p' and 'atttype' to update on next start */
UPDATE @extschema@.pathman_config SET upd_expr = TRUE;

DROP FUNCTION @extschema@.common_relation_checks(REGCLASS, TEXT);
CREATE OR REPLACE FUNCTION @extschema@.common_relation_checks(
	relation		REGCLASS,
	expression		TEXT)
RETURNS BOOLEAN AS
$$
DECLARE
	v_rec			RECORD;
	is_referenced	BOOLEAN;
	rel_persistence	CHAR;

BEGIN
	/* Ignore temporary tables */
	SELECT relpersistence FROM pg_catalog.pg_class
	WHERE oid = relation INTO rel_persistence;

	IF rel_persistence = 't'::CHAR THEN
		RAISE EXCEPTION 'temporary table "%" cannot be partitioned',
						relation::TEXT;
	END IF;

	IF EXISTS (SELECT * FROM @extschema@.pathman_config
			   WHERE partrel = relation) THEN
		RAISE EXCEPTION 'relation "%" has already been partitioned', relation;
	END IF;

	/* Check if there are foreign keys that reference the relation */
	FOR v_rec IN (SELECT * FROM pg_catalog.pg_constraint
				  WHERE confrelid = relation::REGCLASS::OID)
	LOOP
		is_referenced := TRUE;
		RAISE WARNING 'foreign key "%" references relation "%"',
				v_rec.conname, relation;
	END LOOP;

	IF is_referenced THEN
		RAISE EXCEPTION 'relation "%" is referenced from other relations', relation;
	END IF;

	RETURN FALSE;
END
$$
LANGUAGE plpgsql;

DROP FUNCTION @extschema@.build_check_constraint_name(REGCLASS, INT2);
DROP FUNCTION @extschema@.build_check_constraint_name(REGCLASS, TEXT);

CREATE OR REPLACE FUNCTION @extschema@.build_check_constraint_name(
	partition_relid	REGCLASS)
RETURNS TEXT AS 'pg_pathman', 'build_check_constraint_name'
LANGUAGE C STRICT;

DROP FUNCTION @extschema@.add_to_pathman_config(REGCLASS, TEXT, TEXT);

CREATE OR REPLACE FUNCTION @extschema@.add_to_pathman_config(
	parent_relid		REGCLASS,
	attname				TEXT,
	range_interval		TEXT DEFAULT NULL,
	refresh_part_info	BOOL DEFAULT TRUE,
	parttype			INT4 DEFAULT 0
)
RETURNS BOOLEAN AS 'pg_pathman', 'add_to_pathman_config'
LANGUAGE C;

DROP FUNCTION @extschema@.create_hash_partitions(REGCLASS, TEXT, INT4, BOOLEAN,
	TEXT[], TEXT[]);

CREATE OR REPLACE FUNCTION @extschema@.create_hash_partitions(
	parent_relid		REGCLASS,
	expression			TEXT,
	partitions_count	INT4,
	partition_data		BOOLEAN DEFAULT TRUE,
	partition_names		TEXT[] DEFAULT NULL,
	tablespaces			TEXT[] DEFAULT NULL)
RETURNS INTEGER AS
$$
BEGIN
	PERFORM @extschema@.validate_relname(parent_relid);

	IF partition_data = true THEN
		/* Acquire data modification lock */
		PERFORM @extschema@.prevent_relation_modification(parent_relid);
	ELSE
		/* Acquire lock on parent */
		PERFORM @extschema@.lock_partitioned_relation(parent_relid);
	END IF;

	expression := lower(expression);
	PERFORM @extschema@.common_relation_checks(parent_relid, expression);

	/* Insert new entry to pathman config */
	PERFORM @extschema@.add_to_pathman_config(parent_relid, expression, NULL, false);

	/* Create partitions */
	PERFORM @extschema@.create_hash_partitions_internal(parent_relid,
														expression,
														partitions_count,
														partition_names,
														tablespaces);

	/* Notify backend about changes */
	PERFORM @extschema@.on_create_partitions(parent_relid);

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

DROP FUNCTION @extschema@.build_hash_condition(REGTYPE, TEXT, INT4, INT4);

DROP FUNCTION @extschema@.check_boundaries(REGCLASS, TEXT, ANYELEMENT, ANYELEMENT);
CREATE OR REPLACE FUNCTION @extschema@.check_boundaries(
	parent_relid	REGCLASS,
	expression		TEXT,
	start_value		ANYELEMENT,
	end_value		ANYELEMENT)
RETURNS VOID AS
$$
DECLARE
	v_min		start_value%TYPE;
	v_max		start_value%TYPE;
	v_count		BIGINT;

BEGIN
	/* Get min and max values */
	EXECUTE format('SELECT count(*), min(%1$s), max(%1$s)
					FROM %2$s WHERE NOT %1$s IS NULL',
				   expression, parent_relid::TEXT)
	INTO v_count, v_min, v_max;

	/* Check if column has NULL values */
	IF v_count > 0 AND (v_min IS NULL OR v_max IS NULL) THEN
		RAISE EXCEPTION 'expression "%" returns NULL values', expression;
	END IF;

	/* Check lower boundary */
	IF start_value > v_min THEN
		RAISE EXCEPTION 'start value is less than min value of "%"', expression;
	END IF;

	/* Check upper boundary */
	IF end_value <= v_max THEN
		RAISE EXCEPTION 'not enough partitions to fit all values of "%"', expression;
	END IF;
END
$$ LANGUAGE plpgsql;


DROP FUNCTION @extschema@.prepare_for_partitioning(REGCLASS, TEXT, BOOLEAN);
CREATE OR REPLACE FUNCTION @extschema@.prepare_for_partitioning(
	parent_relid	REGCLASS,
	expression		TEXT,
	partition_data	BOOLEAN)
RETURNS VOID AS
$$
BEGIN
	PERFORM @extschema@.validate_relname(parent_relid);

	IF partition_data = true THEN
		/* Acquire data modification lock */
		PERFORM @extschema@.prevent_relation_modification(parent_relid);
	ELSE
		/* Acquire lock on parent */
		PERFORM @extschema@.lock_partitioned_relation(parent_relid);
	END IF;

	expression := lower(expression);
	PERFORM @extschema@.common_relation_checks(parent_relid, expression);
END
$$ LANGUAGE plpgsql;

/*
 * Creates RANGE partitions for specified relation based on datetime attribute
 */
DROP FUNCTION @extschema@.create_range_partitions(REGCLASS, TEXT, ANYELEMENT,
	INTERVAL, INTEGER, BOOLEAN);

CREATE OR REPLACE FUNCTION @extschema@.create_range_partitions(
	parent_relid	REGCLASS,
	expression		TEXT,
	start_value		ANYELEMENT,
	p_interval		INTERVAL,
	p_count			INTEGER DEFAULT NULL,
	partition_data	BOOLEAN DEFAULT TRUE)
RETURNS INTEGER AS
$$
DECLARE
	v_rows_count	BIGINT;
	v_atttype		REGTYPE;
	v_max			start_value%TYPE;
	v_cur_value		start_value%TYPE := start_value;
	end_value		start_value%TYPE;
	part_count		INTEGER := 0;
	i				INTEGER;

BEGIN
	expression := lower(expression);
	PERFORM @extschema@.prepare_for_partitioning(parent_relid, expression, partition_data);

	IF p_count < 0 THEN
		RAISE EXCEPTION '"p_count" must not be less than 0';
	END IF;

	/* Try to determine partitions count if not set */
	IF p_count IS NULL THEN
		EXECUTE format('SELECT count(*), max(%s) FROM %s', expression, parent_relid)
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

	v_atttype := @extschema@.get_base_type(pg_typeof(start_value));

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
		EXECUTE format('SELECT @extschema@.check_boundaries(''%s'', $1, ''%s'', ''%s''::%s)',
			   parent_relid,
			   start_value,
			   end_value,
			   v_atttype::TEXT)
		USING
				expression;
	END IF;

	/* Insert new entry to pathman config */
	PERFORM @extschema@.add_to_pathman_config(parent_relid, expression,
		p_interval::TEXT, false);

	/* Create sequence for child partitions names */
	PERFORM @extschema@.create_or_replace_sequence(parent_relid)
	FROM @extschema@.get_plain_schema_and_relname(parent_relid);

	IF p_count != 0 THEN
		part_count := @extschema@.create_range_partitions_internal(
									parent_relid,
									@extschema@.generate_range_bounds(start_value,
																	  p_interval,
																	  p_count),
									NULL,
									NULL);
	END IF;

	/* Notify backend about changes */
	PERFORM @extschema@.on_create_partitions(parent_relid);

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
DROP FUNCTION @extschema@.create_range_partitions(REGCLASS, TEXT, ANYELEMENT,
	ANYELEMENT, INTEGER, BOOLEAN);

CREATE OR REPLACE FUNCTION @extschema@.create_range_partitions(
	parent_relid	REGCLASS,
	expression		TEXT,
	start_value		ANYELEMENT,
	p_interval		ANYELEMENT,
	p_count			INTEGER DEFAULT NULL,
	partition_data	BOOLEAN DEFAULT TRUE)
RETURNS INTEGER AS
$$
DECLARE
	v_rows_count	BIGINT;
	v_max			start_value%TYPE;
	v_cur_value		start_value%TYPE := start_value;
	end_value		start_value%TYPE;
	part_count		INTEGER := 0;
	i				INTEGER;

BEGIN
	expression := lower(expression);
	PERFORM @extschema@.prepare_for_partitioning(parent_relid, expression, partition_data);

	IF p_count < 0 THEN
		RAISE EXCEPTION 'partitions count must not be less than zero';
	END IF;

	/* Try to determine partitions count if not set */
	IF p_count IS NULL THEN
		EXECUTE format('SELECT count(*), max(%s) FROM %s', expression, parent_relid)
		INTO v_rows_count, v_max;

		IF v_rows_count = 0 THEN
			RAISE EXCEPTION 'cannot determine partitions count for empty table';
		END IF;

		IF v_max IS NULL THEN
			RAISE EXCEPTION 'expression "%" can return NULL values', expression;
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

	/* Insert new entry to pathman config */
	PERFORM @extschema@.add_to_pathman_config(parent_relid, expression,
		p_interval::TEXT, false);

	/* Create sequence for child partitions names */
	PERFORM @extschema@.create_or_replace_sequence(parent_relid)
	FROM @extschema@.get_plain_schema_and_relname(parent_relid);

	IF p_count != 0 THEN
		part_count := @extschema@.create_range_partitions_internal(
						parent_relid,
						@extschema@.generate_range_bounds(start_value, p_interval, p_count),
						NULL,
						NULL);
	END IF;

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
 * Creates RANGE partitions for specified relation based on bounds array
 */
DROP FUNCTION @extschema@.create_range_partitions(REGCLASS, TEXT, ANYARRAY,
	TEXT[], TEXT[], BOOLEAN);

CREATE OR REPLACE FUNCTION @extschema@.create_range_partitions(
	parent_relid	REGCLASS,
	expression		TEXT,
	bounds			ANYARRAY,
	partition_names	TEXT[] DEFAULT NULL,
	tablespaces		TEXT[] DEFAULT NULL,
	partition_data	BOOLEAN DEFAULT TRUE)
RETURNS INTEGER AS
$$
DECLARE
	part_count	INTEGER;
BEGIN
	IF array_ndims(bounds) > 1 THEN
		RAISE EXCEPTION 'Bounds array must be a one dimensional array';
	END IF;

	IF array_length(bounds, 1) < 2 THEN
		RAISE EXCEPTION 'Bounds array must have at least two values';
	END IF;

	expression := lower(expression);
	PERFORM @extschema@.prepare_for_partitioning(parent_relid, expression, partition_data);

	/* Check boundaries */
	PERFORM @extschema@.check_boundaries(parent_relid,
										 expression,
										 bounds[0],
										 bounds[array_length(bounds, 1) - 1]);

	/* Insert new entry to pathman config */
	PERFORM @extschema@.add_to_pathman_config(parent_relid, expression, NULL, false, 2);

	/* Create sequence for child partitions names */
	PERFORM @extschema@.create_or_replace_sequence(parent_relid)
	FROM @extschema@.get_plain_schema_and_relname(parent_relid);

	/* Create partitions */
	part_count := @extschema@.create_range_partitions_internal(parent_relid,
															   bounds,
															   partition_names,
															   tablespaces);

	/* Notify backend about changes */
	PERFORM @extschema@.on_create_partitions(parent_relid);

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
 * Creates RANGE partitions for specified range
 */
DROP FUNCTION @extschema@.create_partitions_from_range(REGCLASS, TEXT, ANYELEMENT,
	ANYELEMENT, ANYELEMENT, BOOLEAN);

CREATE OR REPLACE FUNCTION @extschema@.create_partitions_from_range(
	parent_relid	REGCLASS,
	expression		TEXT,
	start_value		ANYELEMENT,
	end_value		ANYELEMENT,
	p_interval		ANYELEMENT,
	partition_data	BOOLEAN DEFAULT TRUE)
RETURNS INTEGER AS
$$
DECLARE
	part_count		INTEGER := 0;

BEGIN
	expression := lower(expression);
	PERFORM @extschema@.prepare_for_partitioning(parent_relid, expression, partition_data);

	/* Check boundaries */
	PERFORM @extschema@.check_boundaries(parent_relid,
										 expression,
										 start_value,
										 end_value);

	/* Insert new entry to pathman config */
	PERFORM @extschema@.add_to_pathman_config(parent_relid, expression,
		p_interval::TEXT, false);

	/* Create sequence for child partitions names */
	PERFORM @extschema@.create_or_replace_sequence(parent_relid)
	FROM @extschema@.get_plain_schema_and_relname(parent_relid);

	WHILE start_value <= end_value
	LOOP
		PERFORM @extschema@.create_single_range_partition(
			parent_relid,
			start_value,
			start_value + p_interval,
			tablespace := @extschema@.get_tablespace(parent_relid));

		start_value := start_value + p_interval;
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
 * Creates RANGE partitions for specified range based on datetime expression
 */
DROP FUNCTION @extschema@.create_partitions_from_range(REGCLASS, TEXT,
	ANYELEMENT, ANYELEMENT, INTERVAL, BOOLEAN);

CREATE OR REPLACE FUNCTION @extschema@.create_partitions_from_range(
	parent_relid	REGCLASS,
	expression		TEXT,
	start_value		ANYELEMENT,
	end_value		ANYELEMENT,
	p_interval		INTERVAL,
	partition_data	BOOLEAN DEFAULT TRUE)
RETURNS INTEGER AS
$$
DECLARE
	part_count		INTEGER := 0;

BEGIN
	expression := lower(expression);
	PERFORM @extschema@.prepare_for_partitioning(parent_relid, expression,
		partition_data);

	/* Check boundaries */
	PERFORM @extschema@.check_boundaries(parent_relid,
										 expression,
										 start_value,
										 end_value);

	/* Insert new entry to pathman config */
	PERFORM @extschema@.add_to_pathman_config(parent_relid, expression,
		p_interval::TEXT, false);

	/* Create sequence for child partitions names */
	PERFORM @extschema@.create_or_replace_sequence(parent_relid)
	FROM @extschema@.get_plain_schema_and_relname(parent_relid);

	WHILE start_value <= end_value
	LOOP
		EXECUTE
			format('SELECT @extschema@.create_single_range_partition($1, $2, $3::%s, tablespace:=$4);',
				   @extschema@.get_base_type(pg_typeof(start_value))::TEXT)
		USING
			parent_relid,
			start_value,
			start_value + p_interval,
			@extschema@.get_tablespace(parent_relid);

		start_value := start_value + p_interval;
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

DROP FUNCTION @extschema@.build_range_condition(REGCLASS, TEXT,
	ANYELEMENT, ANYELEMENT);

CREATE OR REPLACE FUNCTION @extschema@.build_range_condition(
	partition_relid	REGCLASS,
	expression		TEXT,
	start_value		ANYELEMENT,
	end_value		ANYELEMENT)
RETURNS TEXT AS 'pg_pathman', 'build_range_condition'
LANGUAGE C;
