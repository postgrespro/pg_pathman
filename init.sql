/* ------------------------------------------------------------------------
 *
 * init.sql
 *		Creates config table and provides common utility functions
 *
 * Copyright (c) 2015-2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

/*
 * Pathman config
 *		partrel - regclass (relation type, stored as Oid)
 *		attname - partitioning key
 *		parttype - partitioning type:
 *			1 - HASH
 *			2 - RANGE
 *		range_interval - base interval for RANGE partitioning as string
 */
CREATE TABLE IF NOT EXISTS @extschema@.pathman_config (
	id				SERIAL PRIMARY KEY,
	partrel			REGCLASS NOT NULL,
	attname			TEXT NOT NULL,
	parttype		INTEGER NOT NULL,
	range_interval	TEXT,

	CHECK (parttype IN (1, 2)) /* check for allowed part types */
);

CREATE TABLE IF NOT EXISTS @extschema@.pathman_config_params (
	partrel			REGCLASS NOT NULL,
	enable_parent	BOOLEAN NOT NULL DEFAULT TRUE
);
CREATE UNIQUE INDEX i_pathman_config_params
ON @extschema@.pathman_config_params(partrel);

SELECT pg_catalog.pg_extension_config_dump('@extschema@.pathman_config', '');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.pathman_config_params', '');


CREATE OR REPLACE FUNCTION @extschema@.on_enable_parent(relid OID)
RETURNS OID AS 'pg_pathman' LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION @extschema@.on_disable_parent(relid OID)
RETURNS OID AS 'pg_pathman' LANGUAGE C STRICT;

/* Include parent relation into query plan's for specified relation */
CREATE OR REPLACE FUNCTION @extschema@.enable_parent(relation REGCLASS)
RETURNS VOID AS
$$
BEGIN
	INSERT INTO @extschema@.pathman_config_params values (relation, True)
	ON CONFLICT (partrel) DO
	UPDATE SET enable_parent = True;

	PERFORM @extschema@.on_enable_parent(relation::oid);
END
$$
LANGUAGE plpgsql;

/* Do not include parent relation into query plan's for specified relation */
CREATE OR REPLACE FUNCTION @extschema@.disable_parent(relation REGCLASS)
RETURNS VOID AS
$$
BEGIN
	INSERT INTO @extschema@.pathman_config_params values (relation, False)
	ON CONFLICT (partrel) DO
	UPDATE SET enable_parent = False;

	PERFORM @extschema@.on_disable_parent(relation::oid);
END
$$
LANGUAGE plpgsql;

/*
 * Partitioning data tools
 */
CREATE OR REPLACE FUNCTION @extschema@.active_workers()
RETURNS TABLE (
	pid       INT,
	dbid      INT,
	relid     INT,
	processed INT,
	status    TEXT
) AS 'pg_pathman' LANGUAGE C STRICT;

CREATE OR REPLACE VIEW @extschema@.pathman_active_workers
AS SELECT * FROM @extschema@.active_workers();

CREATE OR REPLACE FUNCTION @extschema@.partition_data_worker(relation regclass)
RETURNS VOID AS 'pg_pathman' LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION @extschema@.stop_worker(relation regclass)
RETURNS BOOL AS 'pg_pathman' LANGUAGE C STRICT;

/* PathmanRange type */
CREATE OR REPLACE FUNCTION @extschema@.pathman_range_in(cstring)
    RETURNS PathmanRange
    AS 'pg_pathman'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION @extschema@.pathman_range_out(PathmanRange)
    RETURNS cstring
    AS 'pg_pathman'
    LANGUAGE C IMMUTABLE STRICT;

/*
CREATE OR REPLACE FUNCTION @extschema@.get_whole_range(relid OID)
    RETURNS PathmanRange
    AS 'pg_pathman'
    LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION @extschema@.range_value_cmp(range PathmanRange, value ANYELEMENT)
	RETURNS INTEGER
	AS 'pg_pathman'
	LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION @extschema@.range_lower(range PathmanRange, dummy ANYELEMENT)
	RETURNS ANYELEMENT
	AS 'pg_pathman'
	LANGUAGE C;

CREATE OR REPLACE FUNCTION @extschema@.range_upper(range PathmanRange, dummy ANYELEMENT)
	RETURNS ANYELEMENT
	AS 'pg_pathman'
	LANGUAGE C;

CREATE OR REPLACE FUNCTION @extschema@.range_oid(range PathmanRange)
	RETURNS OID
	AS 'pg_pathman'
	LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION @extschema@.range_partitions_list(parent_relid OID)
	RETURNS SETOF PATHMANRANGE AS 'pg_pathman'
	LANGUAGE C STRICT;
*/
CREATE TYPE @extschema@.PathmanRange (
	internallength = 32,
	input = pathman_range_in,
	output = pathman_range_out
);

/*
 * Copy rows to partitions
 */
CREATE OR REPLACE FUNCTION @extschema@.partition_data(
    p_relation regclass
    , p_min ANYELEMENT DEFAULT NULL::text
    , p_max ANYELEMENT DEFAULT NULL::text
    , p_limit INT DEFAULT NULL
    , OUT p_total BIGINT)
AS
$$
DECLARE
    v_attr         TEXT;
    v_limit_clause TEXT := '';
    v_where_clause TEXT := '';
BEGIN
    SELECT attname INTO v_attr
    FROM @extschema@.pathman_config WHERE partrel = p_relation;

    PERFORM @extschema@.debug_capture();

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
    EXECUTE format('
        WITH data AS (
            DELETE FROM ONLY %1$s WHERE ctid IN (
                SELECT ctid FROM ONLY %1$s %2$s %3$s FOR UPDATE NOWAIT
            ) RETURNING *)
        INSERT INTO %1$s SELECT * FROM data'
        , p_relation, v_where_clause, v_limit_clause)
    USING p_min, p_max;

    GET DIAGNOSTICS p_total = ROW_COUNT;
    RETURN;
END
$$
LANGUAGE plpgsql;

/*
 * Disable pathman partitioning for specified relation
 */
CREATE OR REPLACE FUNCTION @extschema@.disable_partitioning(
	parent_relid	REGCLASS)
RETURNS VOID AS
$$
BEGIN
	PERFORM @extschema@.validate_relname(parent_relid);

	DELETE FROM @extschema@.pathman_config WHERE partrel = parent_relid;
	PERFORM @extschema@.drop_triggers(parent_relid);

	/* Notify backend about changes */
	PERFORM @extschema@.on_remove_partitions(parent_relid);
END
$$
LANGUAGE plpgsql;

/*
 * Aggregates several common relation checks before partitioning. Suitable for every partitioning type.
 */
CREATE OR REPLACE FUNCTION @extschema@.common_relation_checks(
	p_relation		REGCLASS,
	p_attribute		TEXT)
RETURNS BOOLEAN AS
$$
DECLARE
	v_rec			RECORD;
	is_referenced	BOOLEAN;

BEGIN
	IF EXISTS (SELECT * FROM @extschema@.pathman_config
			   WHERE partrel = p_relation) THEN
		RAISE EXCEPTION 'Relation "%" has already been partitioned', p_relation;
	END IF;

	IF @extschema@.is_attribute_nullable(p_relation, p_attribute) THEN
		RAISE EXCEPTION 'Partitioning key ''%'' must be NOT NULL', p_attribute;
	END IF;

	/* Check if there are foreign keys reference to the relation */
	FOR v_rec IN (SELECT *
				  FROM pg_constraint WHERE confrelid = p_relation::regclass::oid)
	LOOP
		is_referenced := TRUE;
		RAISE WARNING 'Foreign key ''%'' references to the relation ''%''',
				v_rec.conname, p_relation;
	END LOOP;

	IF is_referenced THEN
		RAISE EXCEPTION 'Relation ''%'' is referenced from other relations', p_relation;
	END IF;

	RETURN TRUE;
END
$$
LANGUAGE plpgsql;

/*
 * Returns relname without quotes or something
 */
CREATE OR REPLACE FUNCTION @extschema@.get_plain_schema_and_relname(
	cls				REGCLASS,
	OUT schema		TEXT,
	OUT relname		TEXT)
AS
$$
BEGIN
	SELECT pg_catalog.pg_class.relnamespace::regnamespace,
		   pg_catalog.pg_class.relname
	FROM pg_catalog.pg_class WHERE oid = cls::oid
	INTO schema, relname;
END
$$
LANGUAGE plpgsql;

/*
 * Returns schema-qualified name for table
 */
CREATE OR REPLACE FUNCTION @extschema@.get_schema_qualified_name(
	cls			REGCLASS,
	delimiter	TEXT DEFAULT '.',
	suffix		TEXT DEFAULT '')
RETURNS TEXT AS
$$
BEGIN
	RETURN (SELECT quote_ident(relnamespace::regnamespace::text) ||
				   delimiter ||
				   quote_ident(relname || suffix)
			FROM pg_catalog.pg_class
			WHERE oid = cls::oid);
END
$$
LANGUAGE plpgsql;

/*
 * Validates relation name. It must be schema qualified
 */
CREATE OR REPLACE FUNCTION @extschema@.validate_relname(
	cls		REGCLASS)
RETURNS TEXT AS
$$
DECLARE
	relname	TEXT;

BEGIN
	relname = @extschema@.get_schema_qualified_name(cls);

	IF relname IS NULL THEN
		RAISE EXCEPTION 'Relation %s does not exist', cls;
	END IF;

	RETURN relname;
END
$$
LANGUAGE plpgsql;

/*
 * Check if two relations have equal structures
 */
CREATE OR REPLACE FUNCTION @extschema@.validate_relations_equality(
	relation1 OID, relation2 OID)
RETURNS BOOLEAN AS
$$
DECLARE
	rec	RECORD;

BEGIN
	FOR rec IN (
		WITH
			a1 AS (select * from pg_catalog.pg_attribute
				   where attrelid = relation1 and attnum > 0),
			a2 AS (select * from pg_catalog.pg_attribute
				   where attrelid = relation2 and attnum > 0)
		SELECT a1.attname name1, a2.attname name2, a1.atttypid type1, a2.atttypid type2
		FROM a1
		FULL JOIN a2 ON a1.attnum = a2.attnum
	)
	LOOP
		IF rec.name1 IS NULL OR rec.name2 IS NULL OR rec.name1 != rec.name2 THEN
			RETURN false;
		END IF;
	END LOOP;

	RETURN true;
END
$$
LANGUAGE plpgsql;

/*
 * DDL trigger that deletes entry from pathman_config table
 */
CREATE OR REPLACE FUNCTION @extschema@.pathman_ddl_trigger_func()
RETURNS event_trigger AS
$$
DECLARE
	obj				record;
	pg_class_oid	oid;

BEGIN
	pg_class_oid = 'pg_catalog.pg_class'::regclass;

	/* Handle 'DROP TABLE' events */
	WITH to_be_deleted AS (
		SELECT cfg.partrel AS rel
		FROM pg_event_trigger_dropped_objects() AS events
		JOIN @extschema@.pathman_config AS cfg
		ON cfg.partrel::oid = events.objid
		WHERE events.classid = pg_class_oid
	)
	DELETE FROM @extschema@.pathman_config
	WHERE partrel IN (SELECT rel FROM to_be_deleted);
END
$$
LANGUAGE plpgsql;

/*
 * Drop trigger
 */
CREATE OR REPLACE FUNCTION @extschema@.drop_triggers(
	parent_relid	REGCLASS)
RETURNS VOID AS
$$
DECLARE
	funcname	TEXT;

BEGIN
	funcname := @extschema@.build_update_trigger_func_name(parent_relid);
	EXECUTE format('DROP FUNCTION IF EXISTS %s() CASCADE', funcname);
END
$$ LANGUAGE plpgsql;

/*
 * Drop partitions
 * If delete_data set to TRUE then partitions will be dropped with all the data
 */
CREATE OR REPLACE FUNCTION @extschema@.drop_partitions(
	parent_relid	REGCLASS,
	delete_data		BOOLEAN DEFAULT FALSE)
RETURNS INTEGER AS
$$
DECLARE
	v_rec			RECORD;
	v_rows			INTEGER;
	v_part_count	INTEGER := 0;
	conf_num_del	INTEGER;

BEGIN
	PERFORM @extschema@.validate_relname(parent_relid);

	/* Drop trigger first */
	PERFORM @extschema@.drop_triggers(parent_relid);

	WITH config_num_deleted AS (DELETE FROM @extschema@.pathman_config
								WHERE partrel = parent_relid
								RETURNING *)
	SELECT count(*) from config_num_deleted INTO conf_num_del;

	IF conf_num_del = 0 THEN
		RAISE EXCEPTION 'table % has no partitions', parent_relid::text;
	END IF;

	FOR v_rec IN (SELECT inhrelid::regclass::text AS tbl
				  FROM pg_catalog.pg_inherits
				  WHERE inhparent::regclass = parent_relid)
	LOOP
		IF NOT delete_data THEN
			EXECUTE format('WITH part_data AS (DELETE FROM %s RETURNING *)
							INSERT INTO %s SELECT * FROM part_data',
							v_rec.tbl,
							parent_relid::text);
			GET DIAGNOSTICS v_rows = ROW_COUNT;

			/* Show number of copied rows */
			RAISE NOTICE '% rows copied from %', v_rows, v_rec.tbl;
		END IF;

		EXECUTE format('DROP TABLE %s', v_rec.tbl);
		v_part_count := v_part_count + 1;
	END LOOP;

	/* Notify backend about changes */
	PERFORM @extschema@.on_remove_partitions(parent_relid);

	RETURN v_part_count;
END
$$ LANGUAGE plpgsql
SET pg_pathman.enable_partitionfilter = off;



CREATE EVENT TRIGGER pathman_ddl_trigger
ON sql_drop
EXECUTE PROCEDURE @extschema@.pathman_ddl_trigger_func();


/*
 * Check if regclass is date or timestamp
 */
CREATE OR REPLACE FUNCTION @extschema@.is_date_type(
	typid	REGTYPE)
RETURNS BOOLEAN AS 'pg_pathman', 'is_date_type'
LANGUAGE C STRICT;


CREATE OR REPLACE FUNCTION @extschema@.on_create_partitions(
	relid	REGCLASS)
RETURNS VOID AS 'pg_pathman', 'on_partitions_created'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION @extschema@.on_update_partitions(
	relid	REGCLASS)
RETURNS VOID AS 'pg_pathman', 'on_partitions_updated'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION @extschema@.on_remove_partitions(
	relid	REGCLASS)
RETURNS VOID AS 'pg_pathman', 'on_partitions_removed'
LANGUAGE C STRICT;


/*
 * Checks if attribute is nullable
 */
CREATE OR REPLACE FUNCTION @extschema@.is_attribute_nullable(
	REGCLASS, TEXT)
RETURNS BOOLEAN AS 'pg_pathman', 'is_attribute_nullable'
LANGUAGE C STRICT;

/*
 * Returns attribute type name for relation
 */
CREATE OR REPLACE FUNCTION @extschema@.get_attribute_type_name(
	REGCLASS, TEXT)
RETURNS TEXT AS 'pg_pathman', 'get_attribute_type_name'
LANGUAGE C STRICT;

/*
 * Build check constraint name for a specified relation's column
 */
CREATE OR REPLACE FUNCTION @extschema@.build_check_constraint_name(
	REGCLASS, INT2)
RETURNS TEXT AS 'pg_pathman', 'build_check_constraint_name_attnum'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION @extschema@.build_check_constraint_name(
	REGCLASS, TEXT)
RETURNS TEXT AS 'pg_pathman', 'build_check_constraint_name_attname'
LANGUAGE C STRICT;

/*
 * Build update trigger and its underlying function's names.
 */
CREATE OR REPLACE FUNCTION @extschema@.build_update_trigger_name(
	REGCLASS)
RETURNS TEXT AS 'pg_pathman', 'build_update_trigger_name'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION @extschema@.build_update_trigger_func_name(
	REGCLASS)
RETURNS TEXT AS 'pg_pathman', 'build_update_trigger_func_name'
LANGUAGE C STRICT;

/*
 * DEBUG: Place this inside some plpgsql fuction and set breakpoint.
 */
CREATE OR REPLACE FUNCTION @extschema@.debug_capture()
RETURNS VOID AS 'pg_pathman', 'debug_capture'
LANGUAGE C STRICT;

/*
 * Get parent of pg_pathman's partition.
 */
CREATE OR REPLACE FUNCTION @extschema@.get_parent_of_partition(REGCLASS)
RETURNS REGCLASS AS 'pg_pathman', 'get_parent_of_partition_pl'
LANGUAGE C STRICT;
