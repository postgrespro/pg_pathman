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
 * Alter config tables
 * ----------------------------------------------------------------------*/
CREATE OR REPLACE FUNCTION @extschema@.validate_interval_value(
	partrel			REGCLASS,
	attname			TEXT,
	parttype		INTEGER,
	range_interval	TEXT)
RETURNS BOOL AS 'pg_pathman', 'validate_interval_value'
LANGUAGE C;

ALTER TABLE @extschema@.pathman_config
ADD CHECK (@extschema@.validate_interval_value(partrel,
											   attname,
											   parttype,
											   range_interval));

/*
 * Drop check constraint to be able to update column type. We recreate it
 * later and it will be slightly different
 */
DROP FUNCTION @extschema@.validate_part_callback(REGPROC, BOOL) CASCADE;

/* Change type for init_callback attribute */
ALTER TABLE @extschema@.pathman_config_params
ALTER COLUMN init_callback TYPE TEXT,
ALTER COLUMN init_callback DROP NOT NULL,
ALTER COLUMN init_callback SET DEFAULT NULL;

/* Set init_callback to NULL where it used to be 0 */
UPDATE @extschema@.pathman_config_params
SET init_callback = NULL
WHERE init_callback = '-';

CREATE OR REPLACE FUNCTION @extschema@.validate_part_callback(
	callback		REGPROCEDURE,
	raise_error		BOOL DEFAULT TRUE)
RETURNS BOOL AS 'pg_pathman', 'validate_part_callback_pl'
LANGUAGE C STRICT;

ALTER TABLE @extschema@.pathman_config_params
ADD CHECK (@extschema@.validate_part_callback(CASE WHEN init_callback IS NULL
											  THEN 0::REGPROCEDURE
											  ELSE init_callback::REGPROCEDURE
											  END));

/* ------------------------------------------------------------------------
 * Drop irrelevant objects
 * ----------------------------------------------------------------------*/
DROP FUNCTION @extschema@.set_init_callback(REGCLASS, REGPROC);
DROP FUNCTION @extschema@.get_attribute_type(REGCLASS, TEXT);
DROP FUNCTION @extschema@.create_hash_partitions(REGCLASS, TEXT, INTEGER, BOOLEAN);
DROP FUNCTION @extschema@.create_hash_partitions_internal(REGCLASS, TEXT, INTEGER);
DROP FUNCTION @extschema@.build_range_condition(TEXT, ANYELEMENT, ANYELEMENT);
DROP FUNCTION @extschema@.split_range_partition(REGCLASS, ANYELEMENT, TEXT, TEXT, OUT ANYARRAY);
DROP FUNCTION @extschema@.drop_range_partition(REGCLASS, BOOLEAN);
DROP FUNCTION @extschema@.attach_range_partition(REGCLASS, REGCLASS, ANYELEMENT, ANYELEMENT);
DROP FUNCTION @extschema@.detach_range_partition(REGCLASS);
DROP FUNCTION @extschema@.merge_range_partitions_internal(REGCLASS, REGCLASS, REGCLASS, ANYELEMENT);
DROP FUNCTION @extschema@.copy_foreign_keys(REGCLASS, REGCLASS);
DROP FUNCTION @extschema@.invoke_on_partition_created_callback(REGCLASS, REGCLASS, REGPROCEDURE, ANYELEMENT, ANYELEMENT);
DROP FUNCTION @extschema@.invoke_on_partition_created_callback(REGCLASS, REGCLASS, REGPROCEDURE);


/* ------------------------------------------------------------------------
 * Alter functions' modifiers
 * ----------------------------------------------------------------------*/
ALTER FUNCTION @extschema@.pathman_set_param(REGCLASS, TEXT, ANYELEMENT) STRICT;


/* ------------------------------------------------------------------------
 * (Re)create functions
 * ----------------------------------------------------------------------*/

/*
 * Invoke init_callback on RANGE partition.
 */
CREATE OR REPLACE FUNCTION @extschema@.invoke_on_partition_created_callback(
	parent_relid	REGCLASS,
	partition_relid	REGCLASS,
	init_callback	REGPROCEDURE,
	start_value		ANYELEMENT,
	end_value		ANYELEMENT)
RETURNS VOID AS 'pg_pathman', 'invoke_on_partition_created_callback'
LANGUAGE C;


/*
 * Invoke init_callback on HASH partition.
 */
CREATE OR REPLACE FUNCTION @extschema@.invoke_on_partition_created_callback(
	parent_relid	REGCLASS,
	partition_relid	REGCLASS,
	init_callback	REGPROCEDURE)
RETURNS VOID AS 'pg_pathman', 'invoke_on_partition_created_callback'
LANGUAGE C;


/*
 * Copy all of parent's foreign keys.
 */
CREATE OR REPLACE FUNCTION @extschema@.copy_foreign_keys(
	parent_relid	REGCLASS,
	partition_relid	REGCLASS)
RETURNS VOID AS
$$
DECLARE
	rec		RECORD;

BEGIN
	PERFORM @extschema@.validate_relname(parent_relid);
	PERFORM @extschema@.validate_relname(partition_relid);

	FOR rec IN (SELECT oid as conid FROM pg_catalog.pg_constraint
				WHERE conrelid = parent_relid AND contype = 'f')
	LOOP
		EXECUTE format('ALTER TABLE %s ADD %s',
					   partition_relid::TEXT,
					   pg_catalog.pg_get_constraintdef(rec.conid));
	END LOOP;
END
$$ LANGUAGE plpgsql STRICT;


CREATE OR REPLACE FUNCTION @extschema@.set_init_callback(
	relation	REGCLASS,
	callback	REGPROCEDURE DEFAULT 0)
RETURNS VOID AS
$$
DECLARE
	regproc_text	TEXT := NULL;

BEGIN

	/* Fetch schema-qualified name of callback */
	IF callback != 0 THEN
		SELECT quote_ident(nspname) || '.' ||
			   quote_ident(proname) || '(' ||
					(SELECT string_agg(x.argtype::REGTYPE::TEXT, ',')
					 FROM unnest(proargtypes) AS x(argtype)) ||
			   ')'
		FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n
		ON n.oid = p.pronamespace
		WHERE p.oid = callback
		INTO regproc_text; /* <= result */
	END IF;

	PERFORM @extschema@.pathman_set_param(relation, 'init_callback', regproc_text);
END
$$
LANGUAGE plpgsql STRICT;


CREATE OR REPLACE FUNCTION @extschema@.set_interval(
	relation		REGCLASS,
	value			ANYELEMENT)
RETURNS VOID AS
$$
DECLARE
	affected	INTEGER;
BEGIN
	UPDATE @extschema@.pathman_config
	SET range_interval = value::text
	WHERE partrel = relation AND parttype = 2;

	/* Check number of affected rows */
	GET DIAGNOSTICS affected = ROW_COUNT;

	IF affected = 0 THEN
		RAISE EXCEPTION 'table "%" is not partitioned by RANGE', relation;
	END IF;
END
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION @extschema@.alter_partition(
	relation		REGCLASS,
	new_name		TEXT,
	new_schema		REGNAMESPACE,
	new_tablespace	TEXT)
RETURNS VOID AS
$$
DECLARE
	orig_name	TEXT;
	orig_schema	OID;

BEGIN
	SELECT relname, relnamespace FROM pg_class
	WHERE oid = relation
	INTO orig_name, orig_schema;

	/* Alter table name */
	IF new_name != orig_name THEN
		EXECUTE format('ALTER TABLE %s RENAME TO %s', relation, new_name);
	END IF;

	/* Alter table schema */
	IF new_schema != orig_schema THEN
		EXECUTE format('ALTER TABLE %s SET SCHEMA %s', relation, new_schema);
	END IF;

	/* Move to another tablespace */
	IF NOT new_tablespace IS NULL THEN
		EXECUTE format('ALTER TABLE %s SET TABLESPACE %s', relation, new_tablespace);
	END IF;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION @extschema@.get_partition_key(
	relid	REGCLASS)
RETURNS TEXT AS
$$
	SELECT attname FROM pathman_config WHERE partrel = relid;
$$
LANGUAGE sql STRICT;


CREATE OR REPLACE FUNCTION @extschema@.get_partition_key_type(
	relid	REGCLASS)
RETURNS REGTYPE AS 'pg_pathman', 'get_partition_key_type'
LANGUAGE C STRICT;


CREATE OR REPLACE FUNCTION @extschema@.create_hash_partitions(
	parent_relid		REGCLASS,
	attribute			TEXT,
	partitions_count	INTEGER,
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

	attribute := lower(attribute);
	PERFORM @extschema@.common_relation_checks(parent_relid, attribute);

	/* Insert new entry to pathman config */
	INSERT INTO @extschema@.pathman_config (partrel, attname, parttype)
	VALUES (parent_relid, attribute, 1);

	/* Create partitions */
	PERFORM @extschema@.create_hash_partitions_internal(parent_relid,
														attribute,
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
	SELECT init_callback
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
	atttype := @extschema@.get_partition_key_type(parent_relid);

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
	partitions_count	INTEGER,
	partition_names		TEXT[] DEFAULT NULL,
	tablespaces			TEXT[] DEFAULT NULL)
RETURNS VOID AS 'pg_pathman', 'create_hash_partitions_internal'
LANGUAGE C;


CREATE OR REPLACE FUNCTION @extschema@.split_range_partition(
	partition_relid	REGCLASS,
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
	v_parent = @extschema@.get_parent_of_partition(partition_relid);

	/* Acquire lock on parent */
	PERFORM @extschema@.lock_partitioned_relation(v_parent);

	/* Acquire data modification lock (prevent further modifications) */
	PERFORM @extschema@.prevent_relation_modification(partition_relid);

	v_atttype = @extschema@.get_partition_key_type(v_parent);

	SELECT attname, parttype
	FROM @extschema@.pathman_config
	WHERE partrel = v_parent
	INTO v_attname, v_part_type;

	/* Check if this is a RANGE partition */
	IF v_part_type != 2 THEN
		RAISE EXCEPTION '"%" is not a RANGE partition', partition_relid::TEXT;
	END IF;

	/* Get partition values range */
	EXECUTE format('SELECT @extschema@.get_part_range($1, NULL::%s)',
				   @extschema@.get_base_type(v_atttype)::TEXT)
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
	v_new_partition := @extschema@.create_single_range_partition(v_parent,
																 split_value,
																 p_range[2],
																 partition_name,
																 tablespace);

	/* Copy data */
	v_cond := @extschema@.build_range_condition(v_new_partition::regclass,
												v_attname, split_value, p_range[2]);
	EXECUTE format('WITH part_data AS (DELETE FROM %s WHERE %s RETURNING *)
					INSERT INTO %s SELECT * FROM part_data',
				   partition_relid::TEXT,
				   v_cond,
				   v_new_partition);

	/* Alter original partition */
	v_cond := @extschema@.build_range_condition(partition_relid::regclass,
												v_attname, p_range[1], split_value);
	v_check_name := @extschema@.build_check_constraint_name(partition_relid, v_attname);

	EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %s',
				   partition_relid::TEXT,
				   v_check_name);

	EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s)',
				   partition_relid::TEXT,
				   v_check_name,
				   v_cond);

	/* Tell backend to reload configuration */
	PERFORM @extschema@.on_update_partitions(v_parent);
END
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION @extschema@.merge_range_partitions(
	partitions		REGCLASS[])
RETURNS VOID AS 'pg_pathman', 'merge_range_partitions'
LANGUAGE C STRICT;


CREATE OR REPLACE FUNCTION @extschema@.merge_range_partitions(
	partition1		REGCLASS,
	partition2		REGCLASS)
RETURNS VOID AS
$$
BEGIN
	PERFORM @extschema@.merge_range_partitions(array[partition1, partition2]::regclass[]);
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION @extschema@.append_range_partition(
	parent_relid	REGCLASS,
	partition_name	TEXT DEFAULT NULL,
	tablespace		TEXT DEFAULT NULL)
RETURNS TEXT AS
$$
DECLARE
	v_atttype		REGTYPE;
	v_part_name		TEXT;
	v_interval		TEXT;

BEGIN
	PERFORM @extschema@.validate_relname(parent_relid);

	/* Acquire lock on parent */
	PERFORM @extschema@.lock_partitioned_relation(parent_relid);

	v_atttype := @extschema@.get_partition_key_type(parent_relid);

	SELECT range_interval
	FROM @extschema@.pathman_config
	WHERE partrel = parent_relid
	INTO v_interval;

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
	v_args_format	TEXT;

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

	IF p_range[2] IS NULL THEN
		RAISE EXCEPTION 'Cannot append partition because last partition''s range is half open';
	END IF;

	IF @extschema@.is_date_type(p_atttype) THEN
		v_args_format := format('$1, $2, ($2 + $3::interval)::%s, $4, $5', v_atttype::TEXT);
	ELSE
		v_args_format := format('$1, $2, $2 + $3::%s, $4, $5', v_atttype::TEXT);
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
		v_part_name;

	RETURN v_part_name;
END
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION @extschema@.prepend_range_partition(
	parent_relid	REGCLASS,
	partition_name	TEXT DEFAULT NULL,
	tablespace		TEXT DEFAULT NULL)
RETURNS TEXT AS
$$
DECLARE
	v_atttype		REGTYPE;
	v_part_name		TEXT;
	v_interval		TEXT;

BEGIN
	PERFORM @extschema@.validate_relname(parent_relid);

	/* Acquire lock on parent */
	PERFORM @extschema@.lock_partitioned_relation(parent_relid);

	v_atttype := @extschema@.get_partition_key_type(parent_relid);

	SELECT range_interval
	FROM @extschema@.pathman_config
	WHERE partrel = parent_relid
	INTO v_interval;

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
	v_args_format	TEXT;

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

	IF p_range[1] IS NULL THEN
		RAISE EXCEPTION 'Cannot prepend partition because first partition''s range is half open';
	END IF;

	IF @extschema@.is_date_type(p_atttype) THEN
		v_args_format := format('$1, ($2 - $3::interval)::%s, $2, $4, $5', v_atttype::TEXT);
	ELSE
		v_args_format := format('$1, $2 - $3::%s, $2, $4, $5', v_atttype::TEXT);
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
		v_part_name;

	RETURN v_part_name;
END
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION @extschema@.attach_range_partition(
	parent_relid	REGCLASS,
	partition_relid	REGCLASS,
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
	PERFORM @extschema@.validate_relname(partition_relid);

	/* Acquire lock on parent */
	PERFORM @extschema@.lock_partitioned_relation(parent_relid);

	/* Ignore temporary tables */
	SELECT relpersistence FROM pg_catalog.pg_class
	WHERE oid = partition_relid INTO rel_persistence;

	IF rel_persistence = 't'::CHAR THEN
		RAISE EXCEPTION 'temporary table "%" cannot be used as a partition',
						partition_relid::TEXT;
	END IF;

	/* check range overlap */
	PERFORM @extschema@.check_range_available(parent_relid, start_value, end_value);

	IF NOT @extschema@.validate_relations_equality(parent_relid, partition_relid) THEN
		RAISE EXCEPTION 'partition must have the exact same structure as parent';
	END IF;

	/* Set inheritance */
	EXECUTE format('ALTER TABLE %s INHERIT %s', partition_relid, parent_relid);

	v_attname := attname FROM @extschema@.pathman_config WHERE partrel = parent_relid;

	IF v_attname IS NULL THEN
		RAISE EXCEPTION 'table "%" is not partitioned', parent_relid::TEXT;
	END IF;

	/* Set check constraint */
	EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s)',
				   partition_relid::TEXT,
				   @extschema@.build_check_constraint_name(partition_relid, v_attname),
				   @extschema@.build_range_condition(partition_relid,
													 v_attname,
													 start_value,
													 end_value));

	/* Fetch init_callback from 'params' table */
	WITH stub_callback(stub) as (values (0))
	SELECT init_callback
	FROM stub_callback
	LEFT JOIN @extschema@.pathman_config_params AS params
	ON params.partrel = parent_relid
	INTO v_init_callback;

	PERFORM @extschema@.invoke_on_partition_created_callback(parent_relid,
															 partition_relid,
															 v_init_callback,
															 start_value,
															 end_value);

	/* Invalidate cache */
	PERFORM @extschema@.on_update_partitions(parent_relid);

	RETURN partition_relid;
END
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION @extschema@.detach_range_partition(
	partition_relid	REGCLASS)
RETURNS TEXT AS
$$
DECLARE
	v_attname		TEXT;
	parent_relid	REGCLASS;

BEGIN
	parent_relid := @extschema@.get_parent_of_partition(partition_relid);

	/* Acquire lock on parent */
	PERFORM @extschema@.prevent_relation_modification(parent_relid);

	v_attname := attname
	FROM @extschema@.pathman_config
	WHERE partrel = parent_relid;

	IF v_attname IS NULL THEN
		RAISE EXCEPTION 'table "%" is not partitioned', parent_relid::TEXT;
	END IF;

	/* Remove inheritance */
	EXECUTE format('ALTER TABLE %s NO INHERIT %s',
				   partition_relid::TEXT,
				   parent_relid::TEXT);

	/* Remove check constraint */
	EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %s',
				   partition_relid::TEXT,
				   @extschema@.build_check_constraint_name(partition_relid, v_attname));

	/* Invalidate cache */
	PERFORM @extschema@.on_update_partitions(parent_relid);

	RETURN partition_relid;
END
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION @extschema@.drop_range_partition(
	partition_relid	REGCLASS,
	delete_data		BOOLEAN DEFAULT TRUE)
RETURNS TEXT AS
$$
DECLARE
	parent_relid	REGCLASS;
	part_name		TEXT;
	v_relkind		CHAR;
	v_rows			BIGINT;
	v_part_type		INTEGER;

BEGIN
	parent_relid := @extschema@.get_parent_of_partition(partition_relid);
	part_name := partition_relid::TEXT; /* save the name to be returned */

	SELECT parttype
	FROM @extschema@.pathman_config
	WHERE partrel = parent_relid
	INTO v_part_type;

	/* Check if this is a RANGE partition */
	IF v_part_type != 2 THEN
		RAISE EXCEPTION '"%" is not a RANGE partition', partition_relid::TEXT;
	END IF;

	/* Acquire lock on parent */
	PERFORM @extschema@.lock_partitioned_relation(parent_relid);

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

	/* Invalidate cache */
	PERFORM @extschema@.on_update_partitions(parent_relid);

	RETURN part_name;
END
$$
LANGUAGE plpgsql
SET pg_pathman.enable_partitionfilter = off;


CREATE OR REPLACE FUNCTION @extschema@.drop_range_partition_expand_next(
	partition_relid	REGCLASS)
RETURNS VOID AS 'pg_pathman', 'drop_range_partition_expand_next'
LANGUAGE C STRICT;


CREATE OR REPLACE FUNCTION @extschema@.build_range_condition(
	p_relid			REGCLASS,
	attribute		TEXT,
	start_value		ANYELEMENT,
	end_value		ANYELEMENT)
RETURNS TEXT AS 'pg_pathman', 'build_range_condition'
LANGUAGE C;


/*
 * Old school way to distribute rows to partitions.
 */
CREATE OR REPLACE FUNCTION @extschema@.partition_data(
	parent_relid	REGCLASS,
	OUT p_total		BIGINT)
AS
$$
BEGIN
	p_total := 0;

	/* Create partitions and copy rest of the data */
	EXECUTE format('WITH part_data AS (DELETE FROM ONLY %1$s RETURNING *)
					INSERT INTO %1$s SELECT * FROM part_data',
				   parent_relid::TEXT);

	/* Get number of inserted rows */
	GET DIAGNOSTICS p_total = ROW_COUNT;
	RETURN;
END
$$
LANGUAGE plpgsql STRICT
SET pg_pathman.enable_partitionfilter = on;

/*
 * Add a row describing the optional parameter to pathman_config_params.
 */
CREATE OR REPLACE FUNCTION @extschema@.pathman_set_param(
	relation	REGCLASS,
	param		TEXT,
	value		ANYELEMENT)
RETURNS VOID AS
$$
BEGIN
	EXECUTE format('INSERT INTO @extschema@.pathman_config_params
					(partrel, %1$s) VALUES ($1, $2)
					ON CONFLICT (partrel) DO UPDATE SET %1$s = $2', param)
	USING relation, value;
END
$$
LANGUAGE plpgsql;



/* ------------------------------------------------------------------------
 * Final words of wisdom
 * ----------------------------------------------------------------------*/
DO language plpgsql
$$
	BEGIN
		RAISE WARNING 'Don''t forget to execute "SET pg_pathman.enable = t" to activate pg_pathman';
	END
$$;
