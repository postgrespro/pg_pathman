/* ------------------------------------------------------------------------
 *
 * pg_pathman--1.1--1.2.sql
 *		Migration scripts to version 1.2
 *
 * Copyright (c) 2015-2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */


/* ------------------------------------------------------------------------
 * Drop irrelevant objects
 * ----------------------------------------------------------------------*/
DROP INDEX i_pathman_config_params;
DROP FUNCTION @extschema@.partitions_count(REGCLASS);
DROP FUNCTION @extschema@.set_init_callback(REGCLASS, REGPROC);
DROP FUNCTION @extschema@.validate_relname(REGCLASS);
DROP FUNCTION @extschema@.get_schema_qualified_name(REGCLASS, TEXT, TEXT);
DROP FUNCTION @extschema@.get_rel_tablespace_name(REGCLASS);
DROP FUNCTION @extschema@.validate_on_partition_created_callback(REGPROC);
DROP FUNCTION @extschema@.get_sequence_name(TEXT, TEXT);
DROP FUNCTION @extschema@.create_or_replace_sequence(TEXT, TEXT);
DROP FUNCTION @extschema@.create_single_range_partition(REGCLASS, ANYELEMENT, ANYELEMENT, TEXT, TEXT);
DROP FUNCTION @extschema@.check_overlap(REGCLASS, ANYELEMENT, ANYELEMENT);
DROP FUNCTION @extschema@.split_range_partition(REGCLASS, ANYELEMENT, TEXT, OUT ANYARRAY);
DROP FUNCTION @extschema@.invalidate_relcache(OID);

/* drop trigger and its function (PATHMAN_CONFIG_PARAMS) */
DROP TRIGGER pathman_config_params_trigger ON @extschema@.pathman_config_params;
DROP FUNCTION @extschema@.pathman_config_params_trigger_func();


/* ------------------------------------------------------------------------
 * Alter functions' modifiers
 * ----------------------------------------------------------------------*/
ALTER FUNCTION @extschema@.pathman_set_param(REGCLASS, TEXT, ANYELEMENT) STRICT;
ALTER FUNCTION @extschema@.build_range_condition(TEXT, ANYELEMENT, ANYELEMENT) STRICT;


/* ------------------------------------------------------------------------
 * (Re)create functions
 * ----------------------------------------------------------------------*/
CREATE OR REPLACE FUNCTION @extschema@.validate_part_callback(
	callback		REGPROC,
	raise_error		BOOL DEFAULT TRUE)
RETURNS BOOL AS 'pg_pathman', 'validate_part_callback_pl'
LANGUAGE C STRICT;


CREATE OR REPLACE FUNCTION @extschema@.set_init_callback(
	relation	REGCLASS,
	callback	REGPROC DEFAULT 0)
RETURNS VOID AS
$$
BEGIN
	PERFORM @extschema@.pathman_set_param(relation, 'init_callback', callback);
END
$$
LANGUAGE plpgsql STRICT;


CREATE OR REPLACE FUNCTION @extschema@.set_spawn_using_bgw(
	relation	REGCLASS,
	value		BOOLEAN)
RETURNS VOID AS
$$
BEGIN
	PERFORM @extschema@.pathman_set_param(relation, 'spawn_using_bgw', value);
END
$$
LANGUAGE plpgsql STRICT;


CREATE OR REPLACE FUNCTION @extschema@.disable_pathman_for(
	parent_relid	REGCLASS)
RETURNS VOID AS
$$
BEGIN
	PERFORM @extschema@.validate_relname(parent_relid);

	/* Delete rows from both config tables */
	DELETE FROM @extschema@.pathman_config WHERE partrel = parent_relid;
	DELETE FROM @extschema@.pathman_config_params WHERE partrel = parent_relid;

	/* Drop triggers on update */
	PERFORM @extschema@.drop_triggers(parent_relid);

	/* Notify backend about changes */
	PERFORM @extschema@.on_remove_partitions(parent_relid);
END
$$
LANGUAGE plpgsql STRICT;


CREATE OR REPLACE FUNCTION @extschema@.common_relation_checks(
	relation		REGCLASS,
	p_attribute		TEXT)
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

	IF @extschema@.is_attribute_nullable(relation, p_attribute) THEN
		RAISE EXCEPTION 'partitioning key "%" must be NOT NULL', p_attribute;
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

	RETURN TRUE;
END
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION @extschema@.pathman_ddl_trigger_func()
RETURNS event_trigger AS
$$
DECLARE
	obj				record;
	pg_class_oid	oid;
	relids			regclass[];
BEGIN
	pg_class_oid = 'pg_catalog.pg_class'::regclass;

	/* Find relids to remove from config */
	SELECT array_agg(cfg.partrel) INTO relids
	FROM pg_event_trigger_dropped_objects() AS events
	JOIN @extschema@.pathman_config AS cfg ON cfg.partrel::oid = events.objid
	WHERE events.classid = pg_class_oid AND events.objsubid = 0;

	/* Cleanup pathman_config */
	DELETE FROM @extschema@.pathman_config WHERE partrel = ANY(relids);

	/* Cleanup params table too */
	DELETE FROM @extschema@.pathman_config_params WHERE partrel = ANY(relids);
END
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION @extschema@.get_number_of_partitions(
	parent_relid		REGCLASS)
RETURNS INT4 AS 'pg_pathman', 'get_number_of_partitions_pl'
LANGUAGE C STRICT;


CREATE OR REPLACE FUNCTION @extschema@.get_tablespace(
	relid	REGCLASS)
RETURNS TEXT AS 'pg_pathman', 'get_tablespace_pl'
LANGUAGE C STRICT;


CREATE OR REPLACE FUNCTION @extschema@.validate_relname(
	relid	REGCLASS)
RETURNS VOID AS 'pg_pathman', 'validate_relname'
LANGUAGE C;


CREATE OR REPLACE FUNCTION @extschema@.create_hash_partitions(
	parent_relid		REGCLASS,
	attribute			TEXT,
	partitions_count	INTEGER,
	partition_data		BOOLEAN DEFAULT TRUE)
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

	attribute := lower(attribute);
	PERFORM @extschema@.common_relation_checks(parent_relid, attribute);

	/* Insert new entry to pathman config */
	INSERT INTO @extschema@.pathman_config (partrel, attname, parttype)
	VALUES (parent_relid, attribute, 1);

	/* Create partitions */
	PERFORM @extschema@.create_hash_partitions_internal(parent_relid,
														attribute,
														partitions_count);

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


CREATE OR REPLACE FUNCTION @extschema@.replace_hash_partition(
	old_partition		REGCLASS,
	new_partition		REGCLASS,
	lock_parent			BOOL DEFAULT TRUE)
RETURNS REGCLASS AS
$$
DECLARE
	parent_relid		REGCLASS;
	part_attname		TEXT;		/* partitioned column */
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
		PERFORM @extschema@.prevent_relation_modification(parent_relid);
	ELSE
		/* Acquire lock on parent */
		PERFORM @extschema@.lock_partitioned_relation(parent_relid);
	END IF;

	/* Acquire data modification lock (prevent further modifications) */
	PERFORM @extschema@.prevent_relation_modification(old_partition);
	PERFORM @extschema@.prevent_relation_modification(new_partition);

	/* Ignore temporary tables */
	SELECT relpersistence FROM pg_catalog.pg_class
	WHERE oid = new_partition INTO rel_persistence;

	IF rel_persistence = 't'::CHAR THEN
		RAISE EXCEPTION 'temporary table "%" cannot be used as a partition',
						new_partition::TEXT;
	END IF;

	/* Check that new partition has an equal structure as parent does */
	IF NOT @extschema@.validate_relations_equality(parent_relid, new_partition) THEN
		RAISE EXCEPTION 'partition must have the exact same structure as parent';
	END IF;

	/* Get partitioning key */
	part_attname := attname FROM @extschema@.pathman_config WHERE partrel = parent_relid;
	IF part_attname IS NULL THEN
		RAISE EXCEPTION 'table "%" is not partitioned', parent_relid::TEXT;
	END IF;

	/* Fetch name of old_partition's HASH constraint */
	old_constr_name = @extschema@.build_check_constraint_name(old_partition::REGCLASS,
															  part_attname);

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
				   @extschema@.build_check_constraint_name(new_partition::REGCLASS,
														   part_attname),
				   old_constr_def);

	/* Fetch init_callback from 'params' table */
	WITH stub_callback(stub) as (values (0))
	SELECT coalesce(init_callback, 0::REGPROCEDURE)
	FROM stub_callback
	LEFT JOIN @extschema@.pathman_config_params AS params
	ON params.partrel = parent_relid
	INTO p_init_callback;

	/* Finally invoke init_callback */
	PERFORM @extschema@.invoke_on_partition_created_callback(parent_relid,
															 new_partition,
															 p_init_callback);

	/* Invalidate cache */
	PERFORM @extschema@.on_update_partitions(parent_relid);

	RETURN new_partition;
END
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION @extschema@.create_hash_update_trigger(
	parent_relid	REGCLASS)
RETURNS TEXT AS
$$
DECLARE
	func TEXT := 'CREATE OR REPLACE FUNCTION %1$s()
				  RETURNS TRIGGER AS
				  $body$
				  DECLARE
					old_idx		INTEGER; /* partition indices */
					new_idx		INTEGER;

				  BEGIN
					old_idx := @extschema@.get_hash_part_idx(%9$s(OLD.%2$s), %3$s);
					new_idx := @extschema@.get_hash_part_idx(%9$s(NEW.%2$s), %3$s);

					IF old_idx = new_idx THEN
						RETURN NEW;
					END IF;

					EXECUTE format(''DELETE FROM %8$s WHERE %4$s'', old_idx)
					USING %5$s;

					EXECUTE format(''INSERT INTO %8$s VALUES (%6$s)'', new_idx)
					USING %7$s;

					RETURN NULL;
				  END $body$
				  LANGUAGE plpgsql';

	trigger					TEXT := 'CREATE TRIGGER %s
									 BEFORE UPDATE ON %s
									 FOR EACH ROW EXECUTE PROCEDURE %s()';

	att_names				TEXT;
	old_fields				TEXT;
	new_fields				TEXT;
	att_val_fmt				TEXT;
	att_fmt					TEXT;
	attr					TEXT;
	plain_schema			TEXT;
	plain_relname			TEXT;
	child_relname_format	TEXT;
	funcname				TEXT;
	triggername				TEXT;
	atttype					REGTYPE;
	partitions_count		INTEGER;

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
	FROM pg_catalog.pg_attribute
	WHERE attrelid = parent_relid AND attnum > 0
	INTO   att_names,
		   old_fields,
		   new_fields,
		   att_val_fmt,
		   att_fmt;

	partitions_count := @extschema@.get_number_of_partitions(parent_relid);

	/* Build trigger & trigger function's names */
	funcname := @extschema@.build_update_trigger_func_name(parent_relid);
	triggername := @extschema@.build_update_trigger_name(parent_relid);

	/* Build partition name template */
	SELECT * INTO plain_schema, plain_relname
	FROM @extschema@.get_plain_schema_and_relname(parent_relid);

	child_relname_format := quote_ident(plain_schema) || '.' ||
							quote_ident(plain_relname || '_%s');

	/* Fetch base hash function for atttype */
	atttype := @extschema@.get_attribute_type(parent_relid, attr);

	/* Format function definition and execute it */
	EXECUTE format(func, funcname, attr, partitions_count, att_val_fmt,
				   old_fields, att_fmt, new_fields, child_relname_format,
				   @extschema@.get_type_hash_func(atttype)::TEXT);

	/* Create trigger on each partition */
	FOR num IN 0..partitions_count-1
	LOOP
		EXECUTE format(trigger,
					   triggername,
					   format(child_relname_format, num),
					   funcname);
	END LOOP;

	return funcname;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION @extschema@.create_hash_partitions_internal(
	parent_relid		REGCLASS,
	attribute			TEXT,
	partitions_count	INTEGER)
RETURNS VOID AS 'pg_pathman', 'create_hash_partitions_internal'
LANGUAGE C STRICT;


CREATE OR REPLACE FUNCTION @extschema@.build_hash_condition(
	attribute_type		REGTYPE,
	attribute			TEXT,
	partitions_count	INT4,
	partitions_index	INT4)
RETURNS TEXT AS 'pg_pathman', 'build_hash_condition'
LANGUAGE C STRICT;


CREATE OR REPLACE FUNCTION @extschema@.create_or_replace_sequence(
	parent_relid	REGCLASS,
	OUT seq_name	TEXT)
AS $$
BEGIN
	seq_name := @extschema@.build_sequence_name(parent_relid);

	EXECUTE format('DROP SEQUENCE IF EXISTS %s', seq_name);
	EXECUTE format('CREATE SEQUENCE %s START 1', seq_name);
END
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION @extschema@.check_boundaries(
	parent_relid	REGCLASS,
	attribute		TEXT,
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
				   attribute, parent_relid::TEXT)
	INTO v_count, v_min, v_max;

	/* Check if column has NULL values */
	IF v_count > 0 AND (v_min IS NULL OR v_max IS NULL) THEN
		RAISE EXCEPTION 'column "%" contains NULL values', attribute;
	END IF;

	/* Check lower boundary */
	IF start_value > v_min THEN
		RAISE EXCEPTION 'start value is less than min value of "%"', attribute;
	END IF;

	/* Check upper boundary */
	IF end_value <= v_max THEN
		RAISE EXCEPTION 'not enough partitions to fit all values of "%"', attribute;
	END IF;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION @extschema@.create_range_partitions(
	parent_relid	REGCLASS,
	attribute		TEXT,
	start_value		ANYELEMENT,
	p_interval		INTERVAL,
	p_count			INTEGER DEFAULT NULL,
	partition_data	BOOLEAN DEFAULT TRUE)
RETURNS INTEGER AS
$$
DECLARE
	v_rows_count		BIGINT;
	v_atttype			REGTYPE;
	v_max				start_value%TYPE;
	v_cur_value			start_value%TYPE := start_value;
	end_value			start_value%TYPE;
	i					INTEGER;

BEGIN
	PERFORM @extschema@.validate_relname(parent_relid);

	IF partition_data = true THEN
		/* Acquire data modification lock */
		PERFORM @extschema@.prevent_relation_modification(parent_relid);
	ELSE
		/* Acquire lock on parent */
		PERFORM @extschema@.lock_partitioned_relation(parent_relid);
	END IF;

	attribute := lower(attribute);
	PERFORM @extschema@.common_relation_checks(parent_relid, attribute);

	IF p_count < 0 THEN
		RAISE EXCEPTION '"p_count" must not be less than 0';
	END IF;

	/* Try to determine partitions count if not set */
	IF p_count IS NULL THEN
		EXECUTE format('SELECT count(*), max(%s) FROM %s', attribute, parent_relid)
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
		EXECUTE format('SELECT @extschema@.check_boundaries(''%s'', ''%s'', ''%s'', ''%s''::%s)',
					   parent_relid,
					   attribute,
					   start_value,
					   end_value,
					   v_atttype::TEXT);
	END IF;

	/* Insert new entry to pathman config */
	INSERT INTO @extschema@.pathman_config (partrel, attname, parttype, range_interval)
	VALUES (parent_relid, attribute, 2, p_interval::TEXT);

	/* Create sequence for child partitions names */
	PERFORM @extschema@.create_or_replace_sequence(parent_relid)
	FROM @extschema@.get_plain_schema_and_relname(parent_relid);

	/* Create first partition */
	FOR i IN 1..p_count
	LOOP
		EXECUTE
			format('SELECT @extschema@.create_single_range_partition($1, $2, $3::%s, tablespace:=$4)',
				   v_atttype::TEXT)
		USING
			parent_relid,
			start_value,
			start_value + p_interval,
			@extschema@.get_tablespace(parent_relid);

		start_value := start_value + p_interval;
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


CREATE OR REPLACE FUNCTION @extschema@.create_range_partitions(
	parent_relid	REGCLASS,
	attribute		TEXT,
	start_value		ANYELEMENT,
	p_interval		ANYELEMENT,
	p_count			INTEGER DEFAULT NULL,
	partition_data	BOOLEAN DEFAULT TRUE)
RETURNS INTEGER AS
$$
DECLARE
	v_rows_count		BIGINT;
	v_max				start_value%TYPE;
	v_cur_value			start_value%TYPE := start_value;
	end_value			start_value%TYPE;
	i					INTEGER;

BEGIN
	PERFORM @extschema@.validate_relname(parent_relid);

	IF partition_data = true THEN
		/* Acquire data modification lock */
		PERFORM @extschema@.prevent_relation_modification(parent_relid);
	ELSE
		/* Acquire lock on parent */
		PERFORM @extschema@.lock_partitioned_relation(parent_relid);
	END IF;

	attribute := lower(attribute);
	PERFORM @extschema@.common_relation_checks(parent_relid, attribute);

	IF p_count < 0 THEN
		RAISE EXCEPTION 'partitions count must not be less than zero';
	END IF;

	/* Try to determine partitions count if not set */
	IF p_count IS NULL THEN
		EXECUTE format('SELECT count(*), max(%s) FROM %s', attribute, parent_relid)
		INTO v_rows_count, v_max;

		IF v_rows_count = 0 THEN
			RAISE EXCEPTION 'cannot determine partitions count for empty table';
		END IF;

		IF v_max IS NULL THEN
			RAISE EXCEPTION 'column "%" has NULL values', attribute;
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
											 attribute,
											 start_value,
											 end_value);
	END IF;

	/* Insert new entry to pathman config */
	INSERT INTO @extschema@.pathman_config (partrel, attname, parttype, range_interval)
	VALUES (parent_relid, attribute, 2, p_interval::TEXT);

	/* Create sequence for child partitions names */
	PERFORM @extschema@.create_or_replace_sequence(parent_relid)
	FROM @extschema@.get_plain_schema_and_relname(parent_relid);

	/* create first partition */
	FOR i IN 1..p_count
	LOOP
		PERFORM @extschema@.create_single_range_partition(
			parent_relid,
			start_value,
			start_value + p_interval,
			tablespace := @extschema@.get_tablespace(parent_relid));

		start_value := start_value + p_interval;
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


CREATE OR REPLACE FUNCTION @extschema@.create_partitions_from_range(
	parent_relid	REGCLASS,
	attribute		TEXT,
	start_value		ANYELEMENT,
	end_value		ANYELEMENT,
	p_interval		ANYELEMENT,
	partition_data	BOOLEAN DEFAULT TRUE)
RETURNS INTEGER AS
$$
DECLARE
	part_count		INTEGER := 0;

BEGIN
	PERFORM @extschema@.validate_relname(parent_relid);

	IF partition_data = true THEN
		/* Acquire data modification lock */
		PERFORM @extschema@.prevent_relation_modification(parent_relid);
	ELSE
		/* Acquire lock on parent */
		PERFORM @extschema@.lock_partitioned_relation(parent_relid);
	END IF;

	attribute := lower(attribute);
	PERFORM @extschema@.common_relation_checks(parent_relid, attribute);

	IF p_interval <= 0 THEN
		RAISE EXCEPTION 'interval must be positive';
	END IF;

	/* Check boundaries */
	PERFORM @extschema@.check_boundaries(parent_relid,
										 attribute,
										 start_value,
										 end_value);

	/* Insert new entry to pathman config */
	INSERT INTO @extschema@.pathman_config (partrel, attname, parttype, range_interval)
	VALUES (parent_relid, attribute, 2, p_interval::TEXT);

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


CREATE OR REPLACE FUNCTION @extschema@.create_partitions_from_range(
	parent_relid	REGCLASS,
	attribute		TEXT,
	start_value		ANYELEMENT,
	end_value		ANYELEMENT,
	p_interval		INTERVAL,
	partition_data	BOOLEAN DEFAULT TRUE)
RETURNS INTEGER AS
$$
DECLARE
	part_count		INTEGER := 0;

BEGIN
	PERFORM @extschema@.validate_relname(parent_relid);

	IF partition_data = true THEN
		/* Acquire data modification lock */
		PERFORM @extschema@.prevent_relation_modification(parent_relid);
	ELSE
		/* Acquire lock on parent */
		PERFORM @extschema@.lock_partitioned_relation(parent_relid);
	END IF;

	attribute := lower(attribute);
	PERFORM @extschema@.common_relation_checks(parent_relid, attribute);

	/* Check boundaries */
	PERFORM @extschema@.check_boundaries(parent_relid,
										 attribute,
										 start_value,
										 end_value);

	/* Insert new entry to pathman config */
	INSERT INTO @extschema@.pathman_config (partrel, attname, parttype, range_interval)
	VALUES (parent_relid, attribute, 2, p_interval::TEXT);

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


CREATE OR REPLACE FUNCTION @extschema@.merge_range_partitions(
	partition1		REGCLASS,
	partition2		REGCLASS)
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
		RAISE EXCEPTION 'specified partitions are not RANGE partitions';
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


CREATE OR REPLACE FUNCTION @extschema@.split_range_partition(
	partition		REGCLASS,
	split_value		ANYELEMENT,
	partition_name	TEXT DEFAULT NULL,
	tablespace		TEXT DEFAULT NULL,
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
	v_parent = @extschema@.get_parent_of_partition(partition);

	/* Acquire lock on parent */
	PERFORM @extschema@.lock_partitioned_relation(v_parent);

	/* Acquire data modification lock (prevent further modifications) */
	PERFORM @extschema@.prevent_relation_modification(partition);

	SELECT attname, parttype
	FROM @extschema@.pathman_config
	WHERE partrel = v_parent
	INTO v_attname, v_part_type;

	IF v_attname IS NULL THEN
		RAISE EXCEPTION 'table "%" is not partitioned', v_parent::TEXT;
	END IF;

	/* Check if this is a RANGE partition */
	IF v_part_type != 2 THEN
		RAISE EXCEPTION '"%" is not a RANGE partition', partition::TEXT;
	END IF;

	v_atttype = @extschema@.get_attribute_type(v_parent, v_attname);

	/* Get partition values range */
	EXECUTE format('SELECT @extschema@.get_part_range($1, NULL::%s)',
				   @extschema@.get_base_type(v_atttype)::TEXT)
	USING partition
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
	v_new_partition := @extschema@.create_single_range_partition(v_parent,
																 split_value,
																 p_range[2],
																 partition_name,
																 tablespace);

	/* Copy data */
	v_cond := @extschema@.build_range_condition(v_attname, split_value, p_range[2]);
	EXECUTE format('WITH part_data AS (DELETE FROM %s WHERE %s RETURNING *)
					INSERT INTO %s SELECT * FROM part_data',
				   partition::TEXT,
				   v_cond,
				   v_new_partition);

	/* Alter original partition */
	v_cond := @extschema@.build_range_condition(v_attname, p_range[1], split_value);
	v_check_name := @extschema@.build_check_constraint_name(partition, v_attname);

	EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %s',
				   partition::TEXT,
				   v_check_name);

	EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s)',
				   partition::TEXT,
				   v_check_name,
				   v_cond);

	/* Tell backend to reload configuration */
	PERFORM @extschema@.on_update_partitions(v_parent);
END
$$
LANGUAGE plpgsql;


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
	IF @extschema@.get_number_of_partitions(parent_relid) = 0 THEN
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
	IF @extschema@.get_number_of_partitions(parent_relid) = 0 THEN
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


CREATE OR REPLACE FUNCTION @extschema@.add_range_partition(
	parent_relid	REGCLASS,
	start_value		ANYELEMENT,
	end_value		ANYELEMENT,
	partition_name	TEXT DEFAULT NULL,
	tablespace		TEXT DEFAULT NULL)
RETURNS TEXT AS
$$
DECLARE
	v_part_name		TEXT;

BEGIN
	PERFORM @extschema@.validate_relname(parent_relid);

	/* Acquire lock on parent */
	PERFORM @extschema@.lock_partitioned_relation(parent_relid);

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
	v_part_name := @extschema@.create_single_range_partition(parent_relid,
															 start_value,
															 end_value,
															 partition_name,
															 tablespace);
	PERFORM @extschema@.on_update_partitions(parent_relid);

	RETURN v_part_name;
END
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION @extschema@.attach_range_partition(
	parent_relid	REGCLASS,
	partition		REGCLASS,
	start_value		ANYELEMENT,
	end_value		ANYELEMENT)
RETURNS TEXT AS
$$
DECLARE
	v_attname			TEXT;
	rel_persistence		CHAR;
	v_init_callback		REGPROCEDURE;

BEGIN
	PERFORM @extschema@.validate_relname(parent_relid);
	PERFORM @extschema@.validate_relname(partition);

	/* Acquire lock on parent */
	PERFORM @extschema@.lock_partitioned_relation(parent_relid);

	/* Ignore temporary tables */
	SELECT relpersistence FROM pg_catalog.pg_class
	WHERE oid = partition INTO rel_persistence;

	IF rel_persistence = 't'::CHAR THEN
		RAISE EXCEPTION 'temporary table "%" cannot be used as a partition',
						partition::TEXT;
	END IF;

	/* check range overlap */
	PERFORM @extschema@.check_range_available(parent_relid, start_value, end_value);

	IF NOT @extschema@.validate_relations_equality(parent_relid, partition) THEN
		RAISE EXCEPTION 'partition must have the exact same structure as parent';
	END IF;

	/* Set inheritance */
	EXECUTE format('ALTER TABLE %s INHERIT %s', partition, parent_relid);

	v_attname := attname FROM @extschema@.pathman_config WHERE partrel = parent_relid;

	IF v_attname IS NULL THEN
		RAISE EXCEPTION 'table "%" is not partitioned', parent_relid::TEXT;
	END IF;

	/* Set check constraint */
	EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s)',
				   partition::TEXT,
				   @extschema@.build_check_constraint_name(partition, v_attname),
				   @extschema@.build_range_condition(v_attname,
													 start_value,
													 end_value));

	/* Fetch init_callback from 'params' table */
	WITH stub_callback(stub) as (values (0))
	SELECT coalesce(init_callback, 0::REGPROCEDURE)
	FROM stub_callback
	LEFT JOIN @extschema@.pathman_config_params AS params
	ON params.partrel = parent_relid
	INTO v_init_callback;

	PERFORM @extschema@.invoke_on_partition_created_callback(parent_relid,
															 partition,
															 v_init_callback,
															 start_value,
															 end_value);

	/* Invalidate cache */
	PERFORM @extschema@.on_update_partitions(parent_relid);

	RETURN partition;
END
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION @extschema@.create_single_range_partition(
	parent_relid	REGCLASS,
	start_value		ANYELEMENT,
	end_value		ANYELEMENT,
	partition_name	TEXT DEFAULT NULL,
	tablespace		TEXT DEFAULT NULL)
RETURNS REGCLASS AS 'pg_pathman', 'create_single_range_partition_pl'
LANGUAGE C
SET client_min_messages = WARNING;


CREATE OR REPLACE FUNCTION @extschema@.build_sequence_name(
	parent_relid	REGCLASS)
RETURNS TEXT AS 'pg_pathman', 'build_sequence_name'
LANGUAGE C;


CREATE OR REPLACE FUNCTION @extschema@.check_range_available(
	parent_relid	REGCLASS,
	range_min		ANYELEMENT,
	range_max		ANYELEMENT)
RETURNS VOID AS 'pg_pathman', 'check_range_available_pl'
LANGUAGE C;


/* Finally create function and trigger (PATHMAN_CONFIG_PARAMS) */
CREATE OR REPLACE FUNCTION @extschema@.pathman_config_params_trigger_func()
RETURNS TRIGGER AS 'pg_pathman', 'pathman_config_params_trigger_func'
LANGUAGE C;

CREATE TRIGGER pathman_config_params_trigger
BEFORE INSERT OR UPDATE OR DELETE ON @extschema@.pathman_config_params
FOR EACH ROW EXECUTE PROCEDURE @extschema@.pathman_config_params_trigger_func();


CREATE OR REPLACE FUNCTION @extschema@.get_pathman_lib_version()
RETURNS CSTRING AS 'pg_pathman', 'get_pathman_lib_version'
LANGUAGE C STRICT;


/* ------------------------------------------------------------------------
 * Alter tables
 * ----------------------------------------------------------------------*/
ALTER TABLE @extschema@.pathman_config_params ADD COLUMN spawn_using_bgw BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE @extschema@.pathman_config_params ADD CHECK (@extschema@.validate_part_callback(init_callback));


/* ------------------------------------------------------------------------
 * Final words of wisdom
 * ----------------------------------------------------------------------*/
DO language plpgsql
$$
	BEGIN
		RAISE WARNING 'Don''t forget to execute "SET pg_pathman.enable = t" to activate pg_pathman';
	END
$$;