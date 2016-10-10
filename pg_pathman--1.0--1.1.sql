/* ------------------------------------------------------------------------
 *
 * pg_pathman--1.0--1.1.sql
 *		Migration scripts to version 1.1
 *
 * Copyright (c) 2015-2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */


/* ------------------------------------------------------------------------
 * Modify config params table
 * ----------------------------------------------------------------------*/
ALTER TABLE @extschema@.pathman_config_params ADD COLUMN init_callback REGPROCEDURE NOT NULL DEFAULT 0;
ALTER TABLE @extschema@.pathman_config_params ALTER COLUMN enable_parent SET DEFAULT FALSE;


/* ------------------------------------------------------------------------
 * Enable permissions
 * ----------------------------------------------------------------------*/
GRANT SELECT, INSERT, UPDATE, DELETE
ON @extschema@.pathman_config, @extschema@.pathman_config_params
TO public;

CREATE OR REPLACE FUNCTION @extschema@.check_security_policy(relation regclass)
RETURNS BOOL AS 'pg_pathman', 'check_security_policy' LANGUAGE C STRICT;

CREATE POLICY deny_modification ON @extschema@.pathman_config
FOR ALL USING (check_security_policy(partrel));

CREATE POLICY deny_modification ON @extschema@.pathman_config_params
FOR ALL USING (check_security_policy(partrel));

CREATE POLICY allow_select ON @extschema@.pathman_config FOR SELECT USING (true);

CREATE POLICY allow_select ON @extschema@.pathman_config_params FOR SELECT USING (true);

ALTER TABLE @extschema@.pathman_config ENABLE ROW LEVEL SECURITY;
ALTER TABLE @extschema@.pathman_config_params ENABLE ROW LEVEL SECURITY;

GRANT SELECT ON @extschema@.pathman_concurrent_part_tasks TO PUBLIC;


/* ------------------------------------------------------------------------
 * Drop irrelevant functions
 * ----------------------------------------------------------------------*/
DROP FUNCTION @extschema@.pathman_set_param(REGCLASS, TEXT, BOOLEAN);
DROP FUNCTION @extschema@.enable_parent(REGCLASS);
DROP FUNCTION @extschema@.disable_parent(relation REGCLASS);
DROP FUNCTION @extschema@.enable_auto(relation REGCLASS);
DROP FUNCTION @extschema@.disable_auto(relation REGCLASS);
DROP FUNCTION @extschema@.partition_table_concurrently(relation regclass);
DROP FUNCTION @extschema@._partition_data_concurrent(REGCLASS, ANYELEMENT, ANYELEMENT, INT, OUT BIGINT);
DROP FUNCTION @extschema@.common_relation_checks(REGCLASS, TEXT);
DROP FUNCTION @extschema@.get_attribute_type_name(REGCLASS, TEXT);
DROP FUNCTION @extschema@.get_type_hash_func(REGTYPE);
DROP FUNCTION @extschema@.check_boundaries(REGCLASS, TEXT, ANYELEMENT, ANYELEMENT);
DROP FUNCTION @extschema@.create_range_partitions(REGCLASS, TEXT, ANYELEMENT, INTERVAL, INTEGER, BOOLEAN);
DROP FUNCTION @extschema@.create_range_partitions(REGCLASS, TEXT, ANYELEMENT, ANYELEMENT, INTEGER, BOOLEAN);
DROP FUNCTION @extschema@.create_partitions_from_range(REGCLASS, TEXT, ANYELEMENT, ANYELEMENT, ANYELEMENT, BOOLEAN);
DROP FUNCTION @extschema@.create_partitions_from_range(REGCLASS, TEXT, ANYELEMENT, ANYELEMENT, INTERVAL, BOOLEAN);
DROP FUNCTION @extschema@.create_single_range_partition(REGCLASS, ANYELEMENT, ANYELEMENT, TEXT);
DROP FUNCTION @extschema@.split_range_partition(REGCLASS, ANYELEMENT, TEXT, OUT ANYARRAY);
DROP FUNCTION @extschema@.merge_range_partitions(REGCLASS, REGCLASS);
DROP FUNCTION @extschema@.append_range_partition(REGCLASS, TEXT);
DROP FUNCTION @extschema@.append_partition_internal(REGCLASS, TEXT, TEXT, ANYARRAY, TEXT);
DROP FUNCTION @extschema@.prepend_range_partition(REGCLASS, TEXT);
DROP FUNCTION @extschema@.prepend_partition_internal(REGCLASS, TEXT, TEXT, ANYARRAY, TEXT);
DROP FUNCTION @extschema@.add_range_partition(REGCLASS, ANYELEMENT, ANYELEMENT, TEXT);
DROP FUNCTION @extschema@.drop_range_partition(REGCLASS);
DROP FUNCTION @extschema@.attach_range_partition(REGCLASS, REGCLASS, ANYELEMENT, ANYELEMENT);
DROP FUNCTION @extschema@.detach_range_partition(REGCLASS);
DROP FUNCTION @extschema@.build_range_condition(TEXT, ANYELEMENT, ANYELEMENT);
DROP FUNCTION @extschema@.get_range_by_idx(REGCLASS, INTEGER, ANYELEMENT);
DROP FUNCTION @extschema@.get_range_by_part_oid(REGCLASS, REGCLASS, ANYELEMENT);
DROP FUNCTION @extschema@.get_min_range_value(REGCLASS, ANYELEMENT);
DROP FUNCTION @extschema@.get_max_range_value(REGCLASS, ANYELEMENT);


/* ------------------------------------------------------------------------
 * Alter functions' modifiers
 * ----------------------------------------------------------------------*/
ALTER FUNCTION @extschema@.partitions_count(REGCLASS) STRICT;
ALTER FUNCTION @extschema@.partition_data(REGCLASS, OUT	BIGINT) STRICT;
ALTER FUNCTION @extschema@.disable_pathman_for(REGCLASS) STRICT;
ALTER FUNCTION @extschema@.get_plain_schema_and_relname(REGCLASS, OUT TEXT, OUT TEXT) STRICT;
ALTER FUNCTION @extschema@.get_schema_qualified_name(REGCLASS, TEXT, TEXT) STRICT;
ALTER FUNCTION @extschema@.drop_triggers(REGCLASS) STRICT;
ALTER FUNCTION @extschema@.check_overlap(REGCLASS, ANYELEMENT, ANYELEMENT) CALLED ON NULL INPUT;
ALTER FUNCTION @extschema@.find_or_create_range_partition(REGCLASS, ANYELEMENT) CALLED ON NULL INPUT;


/* ------------------------------------------------------------------------
 * Add new views
 * ----------------------------------------------------------------------*/
CREATE OR REPLACE FUNCTION @extschema@.show_partition_list()
RETURNS TABLE (
	 parent		REGCLASS,
	 partition	REGCLASS,
	 parttype	INT4,
	 partattr	TEXT,
	 range_min	TEXT,
	 range_max	TEXT)
AS 'pg_pathman', 'show_partition_list_internal' LANGUAGE C STRICT;

CREATE OR REPLACE VIEW @extschema@.pathman_partition_list
AS SELECT * FROM @extschema@.show_partition_list();

GRANT SELECT ON @extschema@.pathman_partition_list TO PUBLIC;


/* ------------------------------------------------------------------------
 * (Re)create functions
 * ----------------------------------------------------------------------*/
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

CREATE OR REPLACE FUNCTION @extschema@.set_enable_parent(
	relation	REGCLASS,
	value		BOOLEAN)
RETURNS VOID AS
$$
BEGIN
	PERFORM @extschema@.pathman_set_param(relation, 'enable_parent', value);
END
$$
LANGUAGE plpgsql STRICT;

/*
 * Partition table using ConcurrentPartWorker.
 */
CREATE OR REPLACE FUNCTION @extschema@.partition_table_concurrently(
	relation		REGCLASS,
	batch_size		INTEGER DEFAULT 1000,
	sleep_time		FLOAT8 DEFAULT 1.0)
RETURNS VOID AS 'pg_pathman', 'partition_table_concurrently'
LANGUAGE C STRICT;

/*
 * Copy rows to partitions concurrently.
 */
CREATE OR REPLACE FUNCTION @extschema@._partition_data_concurrent(
	relation		REGCLASS,
	p_min			ANYELEMENT DEFAULT NULL::text,
	p_max			ANYELEMENT DEFAULT NULL::text,
	p_limit			INT DEFAULT NULL,
	OUT p_total		BIGINT)
AS
$$
DECLARE
	v_attr			TEXT;
	v_limit_clause	TEXT := '';
	v_where_clause	TEXT := '';
	ctids			TID[];

BEGIN
	SELECT attname INTO v_attr
	FROM @extschema@.pathman_config WHERE partrel = relation;

	p_total := 0;

	/* Format LIMIT clause if needed */
	IF NOT p_limit IS NULL THEN
		v_limit_clause := format('LIMIT %s', p_limit);
	END IF;

	/* Format WHERE clause if needed */
	IF NOT p_min IS NULL THEN
		v_where_clause := format('%1$s >= $1', v_attr);
	END IF;

	IF NOT p_max IS NULL THEN
		IF NOT p_min IS NULL THEN
			v_where_clause := v_where_clause || ' AND ';
		END IF;
		v_where_clause := v_where_clause || format('%1$s < $2', v_attr);
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

	EXECUTE format('
		WITH data AS (
			DELETE FROM ONLY %1$s WHERE ctid = ANY($1) RETURNING *)
		INSERT INTO %1$s SELECT * FROM data',
		relation)
	USING ctids;

	/* Get number of inserted rows */
	GET DIAGNOSTICS p_total = ROW_COUNT;
	RETURN;
END
$$
LANGUAGE plpgsql
SET pg_pathman.enable_partitionfilter = on; /* ensures that PartitionFilter is ON */

/*
 * Aggregates several common relation checks before partitioning.
 * Suitable for every partitioning type.
 */
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
		RAISE EXCEPTION 'partitioning key ''%'' must be NOT NULL', p_attribute;
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

/*
 * DDL trigger that removes entry from pathman_config table.
 */
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
	WHERE events.classid = pg_class_oid;

	/* Cleanup pathman_config */
	DELETE FROM @extschema@.pathman_config WHERE partrel = ANY(relids);

	/* Cleanup params table too */
	DELETE FROM @extschema@.pathman_config_params WHERE partrel = ANY(relids);
END
$$
LANGUAGE plpgsql;

/*
 * Drop partitions. If delete_data set to TRUE, partitions
 * will be dropped with all the data.
 */
CREATE OR REPLACE FUNCTION @extschema@.drop_partitions(
	parent_relid	REGCLASS,
	delete_data		BOOLEAN DEFAULT FALSE)
RETURNS INTEGER AS
$$
DECLARE
	v_rec			RECORD;
	v_rows			BIGINT;
	v_part_count	INTEGER := 0;
	conf_num_del	INTEGER;
	v_relkind		CHAR;

BEGIN
	PERFORM @extschema@.validate_relname(parent_relid);

	/* Drop trigger first */
	PERFORM @extschema@.drop_triggers(parent_relid);

	WITH config_num_deleted AS (DELETE FROM @extschema@.pathman_config
								WHERE partrel = parent_relid
								RETURNING *)
	SELECT count(*) from config_num_deleted INTO conf_num_del;

	DELETE FROM @extschema@.pathman_config_params WHERE partrel = parent_relid;

	IF conf_num_del = 0 THEN
		RAISE EXCEPTION 'relation "%" has no partitions', parent_relid::TEXT;
	END IF;

	FOR v_rec IN (SELECT inhrelid::REGCLASS AS tbl
				  FROM pg_catalog.pg_inherits
				  WHERE inhparent::regclass = parent_relid
				  ORDER BY inhrelid ASC)
	LOOP
		IF NOT delete_data THEN
			EXECUTE format('INSERT INTO %s SELECT * FROM %s',
							parent_relid::TEXT,
							v_rec.tbl::TEXT);
			GET DIAGNOSTICS v_rows = ROW_COUNT;

			/* Show number of copied rows */
			RAISE NOTICE '% rows copied from %', v_rows, v_rec.tbl::TEXT;
		END IF;

		SELECT relkind FROM pg_catalog.pg_class
		WHERE oid = v_rec.tbl
		INTO v_relkind;

		/*
		 * Determine the kind of child relation. It can be either regular
		 * table (r) or foreign table (f). Depending on relkind we use
		 * DROP TABLE or DROP FOREIGN TABLE.
		 */
		IF v_relkind = 'f' THEN
			EXECUTE format('DROP FOREIGN TABLE %s', v_rec.tbl::TEXT);
		ELSE
			EXECUTE format('DROP TABLE %s', v_rec.tbl::TEXT);
		END IF;

		v_part_count := v_part_count + 1;
	END LOOP;

	/* Notify backend about changes */
	PERFORM @extschema@.on_remove_partitions(parent_relid);

	RETURN v_part_count;
END
$$ LANGUAGE plpgsql
SET pg_pathman.enable_partitionfilter = off; /* ensures that PartitionFilter is OFF */

/*
 * Copy all of parent's foreign keys.
 */
CREATE OR REPLACE FUNCTION @extschema@.copy_foreign_keys(
	parent_relid	REGCLASS,
	partition		REGCLASS)
RETURNS VOID AS
$$
DECLARE
	rec		RECORD;

BEGIN
	PERFORM @extschema@.validate_relname(parent_relid);
	PERFORM @extschema@.validate_relname(partition);

	FOR rec IN (SELECT oid as conid FROM pg_catalog.pg_constraint
				WHERE conrelid = parent_relid AND contype = 'f')
	LOOP
		EXECUTE format('ALTER TABLE %s ADD %s',
					   partition::TEXT,
					   pg_catalog.pg_get_constraintdef(rec.conid));
	END LOOP;
END
$$ LANGUAGE plpgsql STRICT;

/*
 * Extract basic type of a domain.
 */
CREATE OR REPLACE FUNCTION @extschema@.get_base_type(REGTYPE)
RETURNS REGTYPE AS 'pg_pathman', 'get_base_type_pl'
LANGUAGE C STRICT;

/*
 * Returns attribute type name for relation.
 */
CREATE OR REPLACE FUNCTION @extschema@.get_attribute_type(
	REGCLASS, TEXT)
RETURNS REGTYPE AS 'pg_pathman', 'get_attribute_type_pl'
LANGUAGE C STRICT;

/*
 * Return tablespace name for specified relation.
 */
CREATE OR REPLACE FUNCTION @extschema@.get_rel_tablespace_name(REGCLASS)
RETURNS TEXT AS 'pg_pathman', 'get_rel_tablespace_name'
LANGUAGE C STRICT;


/*
 * Checks that callback function meets specific requirements. Particularly it
 * must have the only JSONB argument and VOID return type.
 */
CREATE OR REPLACE FUNCTION @extschema@.validate_on_partition_created_callback(
	callback		REGPROC)
RETURNS VOID AS 'pg_pathman', 'validate_on_part_init_callback_pl'
LANGUAGE C STRICT;


/*
 * Invoke init_callback on RANGE partition.
 */
CREATE OR REPLACE FUNCTION @extschema@.invoke_on_partition_created_callback(
	parent_relid	REGCLASS,
	partition		REGCLASS,
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
	partition		REGCLASS,
	init_callback	REGPROCEDURE)
RETURNS VOID AS 'pg_pathman', 'invoke_on_partition_created_callback'
LANGUAGE C;

/*
 * Creates hash partitions for specified relation
 */
CREATE OR REPLACE FUNCTION @extschema@.create_hash_partitions(
	parent_relid		REGCLASS,
	attribute			TEXT,
	partitions_count	INTEGER,
	partition_data		BOOLEAN DEFAULT TRUE)
RETURNS INTEGER AS
$$
DECLARE
	v_child_relname		TEXT;
	v_plain_schema		TEXT;
	v_plain_relname		TEXT;
	v_atttype			REGTYPE;
	v_hashfunc			REGPROC;
	v_init_callback		REGPROCEDURE;

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

	/* Fetch atttype and its hash function */
	v_atttype := @extschema@.get_attribute_type(parent_relid, attribute);
	v_hashfunc := @extschema@.get_type_hash_func(v_atttype);

	SELECT * INTO v_plain_schema, v_plain_relname
	FROM @extschema@.get_plain_schema_and_relname(parent_relid);

	/* Insert new entry to pathman config */
	INSERT INTO @extschema@.pathman_config (partrel, attname, parttype)
	VALUES (parent_relid, attribute, 1);

	/* Create partitions and update pg_pathman configuration */
	FOR partnum IN 0..partitions_count-1
	LOOP
		v_child_relname := format('%s.%s',
								  quote_ident(v_plain_schema),
								  quote_ident(v_plain_relname || '_' || partnum));

		EXECUTE format(
			'CREATE TABLE %1$s (LIKE %2$s INCLUDING ALL) INHERITS (%2$s) TABLESPACE %s',
			v_child_relname,
			parent_relid::TEXT,
			@extschema@.get_rel_tablespace_name(parent_relid));

		EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %s
						CHECK (@extschema@.get_hash_part_idx(%s(%s), %s) = %s)',
					   v_child_relname,
					   @extschema@.build_check_constraint_name(v_child_relname::REGCLASS,
															   attribute),
					   v_hashfunc::TEXT,
					   attribute,
					   partitions_count,
					   partnum);

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
																 v_init_callback);
	END LOOP;

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

/*
 * Creates an update trigger
 */
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

	partitions_count := COUNT(*) FROM pg_catalog.pg_inherits
						WHERE inhparent = parent_relid::oid;

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

	/* Create trigger on every partition */
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

/*
 * Returns hash function OID for specified type
 */
CREATE OR REPLACE FUNCTION @extschema@.get_type_hash_func(REGTYPE)
RETURNS REGPROC AS 'pg_pathman', 'get_type_hash_func'
LANGUAGE C STRICT;

/*
 * Check RANGE partition boundaries.
 */
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
		RAISE EXCEPTION '''%'' column contains NULL values', attribute;
	END IF;

	/* Check lower boundary */
	IF start_value > v_min THEN
		RAISE EXCEPTION 'start value is less than minimum value of ''%''',
				attribute;
	END IF;

	/* Check upper boundary */
	IF end_value <= v_max THEN
		RAISE EXCEPTION 'not enough partitions to fit all values of ''%''',
				attribute;
	END IF;
END
$$ LANGUAGE plpgsql;

/*
 * Creates RANGE partitions for specified relation based on datetime attribute
 */
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
		RAISE EXCEPTION '''p_count'' must not be less than 0';
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

	/* Create sequence for child partitions names */
	PERFORM @extschema@.create_or_replace_sequence(schema, relname)
	FROM @extschema@.get_plain_schema_and_relname(parent_relid);

	/* Insert new entry to pathman config */
	INSERT INTO @extschema@.pathman_config (partrel, attname, parttype, range_interval)
	VALUES (parent_relid, attribute, 2, p_interval::TEXT);

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
			@extschema@.get_rel_tablespace_name(parent_relid);

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

/*
 * Creates RANGE partitions for specified relation based on numerical attribute
 */
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
			RAISE EXCEPTION '''%'' column has NULL values', attribute;
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

	/* Create sequence for child partitions names */
	PERFORM @extschema@.create_or_replace_sequence(schema, relname)
	FROM @extschema@.get_plain_schema_and_relname(parent_relid);

	/* Insert new entry to pathman config */
	INSERT INTO @extschema@.pathman_config (partrel, attname, parttype, range_interval)
	VALUES (parent_relid, attribute, 2, p_interval::TEXT);

	/* create first partition */
	FOR i IN 1..p_count
	LOOP
		PERFORM @extschema@.create_single_range_partition(
			parent_relid,
			start_value,
			start_value + p_interval,
			tablespace := @extschema@.get_rel_tablespace_name(parent_relid));

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

/*
 * Creates RANGE partitions for specified range
 */
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

	/* Create sequence for child partitions names */
	PERFORM @extschema@.create_or_replace_sequence(schema, relname)
	FROM @extschema@.get_plain_schema_and_relname(parent_relid);

	/* Insert new entry to pathman config */
	INSERT INTO @extschema@.pathman_config (partrel, attname, parttype, range_interval)
	VALUES (parent_relid, attribute, 2, p_interval::TEXT);

	WHILE start_value <= end_value
	LOOP
		PERFORM @extschema@.create_single_range_partition(
			parent_relid,
			start_value,
			start_value + p_interval,
			tablespace := @extschema@.get_rel_tablespace_name(parent_relid));

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
 * Creates RANGE partitions for specified range based on datetime attribute
 */
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

	/* Create sequence for child partitions names */
	PERFORM @extschema@.create_or_replace_sequence(schema, relname)
	FROM @extschema@.get_plain_schema_and_relname(parent_relid);

	/* Insert new entry to pathman config */
	INSERT INTO @extschema@.pathman_config (partrel, attname, parttype, range_interval)
	VALUES (parent_relid, attribute, 2, p_interval::TEXT);

	WHILE start_value <= end_value
	LOOP
		EXECUTE
			format('SELECT @extschema@.create_single_range_partition($1, $2, $3::%s, tablespace:=$4);',
				   @extschema@.get_base_type(pg_typeof(start_value))::TEXT)
		USING
			parent_relid,
			start_value,
			start_value + p_interval,
			@extschema@.get_rel_tablespace_name(parent_relid);

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
 * Creates new RANGE partition. Returns partition name.
 * NOTE: This function SHOULD NOT take xact_handling lock (BGWs in 9.5).
 */
CREATE OR REPLACE FUNCTION @extschema@.create_single_range_partition(
	parent_relid	REGCLASS,
	start_value		ANYELEMENT,
	end_value		ANYELEMENT,
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
													 start_value,
													 end_value));

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
															 start_value,
															 end_value);

	RETURN v_child_relname;
END
$$ LANGUAGE plpgsql
SET client_min_messages = WARNING;

/*
 * Split RANGE partition
 */
CREATE OR REPLACE FUNCTION @extschema@.split_range_partition(
	partition		REGCLASS,
	split_value		ANYELEMENT,
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
																 partition_name);

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

/*
 * Merge RANGE partitions
 */
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
	PERFORM @extschema@.validate_relname(parent_relid);

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
	PERFORM @extschema@.validate_relname(parent_relid);

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
	IF @extschema@.partitions_count(parent_relid) > 0
	   AND @extschema@.check_overlap(parent_relid, start_value, end_value) THEN
		RAISE EXCEPTION 'specified range overlaps with existing partitions';
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

/*
 * Drop range partition
 */
CREATE OR REPLACE FUNCTION @extschema@.drop_range_partition(
	partition		REGCLASS,
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
	parent_relid := @extschema@.get_parent_of_partition(partition);
	part_name := partition::TEXT; /* save the name to be returned */

	SELECT parttype
	FROM @extschema@.pathman_config
	WHERE partrel = parent_relid
	INTO v_part_type;

	/* Check if this is a RANGE partition */
	IF v_part_type != 2 THEN
		RAISE EXCEPTION '"%" is not a RANGE partition', partition::TEXT;
	END IF;

	/* Acquire lock on parent */
	PERFORM @extschema@.lock_partitioned_relation(parent_relid);

	IF NOT delete_data THEN
		EXECUTE format('INSERT INTO %s SELECT * FROM %s',
						parent_relid::TEXT,
						partition::TEXT);
		GET DIAGNOSTICS v_rows = ROW_COUNT;

		/* Show number of copied rows */
		RAISE NOTICE '% rows copied from %', v_rows, partition::TEXT;
	END IF;

	SELECT relkind FROM pg_catalog.pg_class
	WHERE oid = partition
	INTO v_relkind;

	/*
	 * Determine the kind of child relation. It can be either regular
	 * table (r) or foreign table (f). Depending on relkind we use
	 * DROP TABLE or DROP FOREIGN TABLE.
	 */
	IF v_relkind = 'f' THEN
		EXECUTE format('DROP FOREIGN TABLE %s', partition::TEXT);
	ELSE
		EXECUTE format('DROP TABLE %s', partition::TEXT);
	END IF;

	/* Invalidate cache */
	PERFORM @extschema@.on_update_partitions(parent_relid);

	RETURN part_name;
END
$$
LANGUAGE plpgsql
SET pg_pathman.enable_partitionfilter = off; /* ensures that PartitionFilter is OFF */

/*
 * Attach range partition
 */
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

	IF @extschema@.check_overlap(parent_relid, start_value, end_value) THEN
		RAISE EXCEPTION 'specified range overlaps with existing partitions';
	END IF;

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

/*
 * Detach range partition
 */
CREATE OR REPLACE FUNCTION @extschema@.detach_range_partition(
	partition		REGCLASS)
RETURNS TEXT AS
$$
DECLARE
	v_attname		TEXT;
	parent_relid	REGCLASS;

BEGIN
	parent_relid := @extschema@.get_parent_of_partition(partition);

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
				   partition::TEXT,
				   parent_relid::TEXT);

	/* Remove check constraint */
	EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %s',
				   partition::TEXT,
				   @extschema@.build_check_constraint_name(partition, v_attname));

	/* Invalidate cache */
	PERFORM @extschema@.on_update_partitions(parent_relid);

	RETURN partition;
END
$$
LANGUAGE plpgsql;

/*
 * Creates an update trigger
 */
CREATE OR REPLACE FUNCTION @extschema@.create_range_update_trigger(
	IN parent_relid	REGCLASS)
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
	start_value		ANYELEMENT,
	end_value		ANYELEMENT)
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
 * Returns min and max values for specified RANGE partition.
 */
CREATE OR REPLACE FUNCTION @extschema@.get_part_range(
	partition_relid	REGCLASS,
	dummy			ANYELEMENT)
RETURNS ANYARRAY AS 'pg_pathman', 'get_part_range_by_oid'
LANGUAGE C;

