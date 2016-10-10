/***********************************************************************
 * Modify config params table
 **********************************************************************/
ALTER TABLE @extschema@.pathman_config_params ADD COLUMN init_callback REGPROCEDURE NOT NULL DEFAULT 0;
ALTER TABLE @extschema@.pathman_config_params ALTER COLUMN enable_parent SET DEFAULT FALSE;

/* Enable permissions */
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

/* Drop irrelevant functions */
DROP FUNCTION @extschema@.invalidate_relcache(OID);
DROP FUNCTION @extschema@.pathman_set_param(REGCLASS, TEXT, BOOLEAN);
DROP FUNCTION @extschema@.enable_parent(REGCLASS);
DROP FUNCTION @extschema@.disable_parent(relation REGCLASS);
DROP FUNCTION @extschema@.enable_auto(relation REGCLASS);
DROP FUNCTION @extschema@.disable_auto(relation REGCLASS);
DROP FUNCTION @extschema@.partition_table_concurrently(relation regclass);
DROP FUNCTION @extschema@._partition_data_concurrent(REGCLASS, ANYELEMENT, ANYELEMENT, INT, OUT BIGINT);
DROP FUNCTION @extschema@.common_relation_checks(REGCLASS, TEXT);

/* Alter functions' modifiers */
ALTER FUNCTION @extschema@.partitions_count(REGCLASS) STRICT;
ALTER FUNCTION @extschema@.partition_data(REGCLASS, OUT	BIGINT) STRICT;
ALTER FUNCTION @extschema@.disable_pathman_for(REGCLASS) STRICT;
ALTER FUNCTION @extschema@.get_plain_schema_and_relname(REGCLASS, OUT TEXT, OUT TEXT) STRICT;

/* Create functions */
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