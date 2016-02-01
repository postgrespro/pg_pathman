/*
 * Relations using partitioning
 */
CREATE TABLE IF NOT EXISTS @extschema@.pathman_config (
    id         SERIAL PRIMARY KEY,
    relname    VARCHAR(127),
    attname    VARCHAR(127),
    parttype   INTEGER
);


CREATE OR REPLACE FUNCTION @extschema@.on_create_partitions(relid OID)
RETURNS VOID AS 'pg_pathman', 'on_partitions_created' LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION @extschema@.on_update_partitions(relid OID)
RETURNS VOID AS 'pg_pathman', 'on_partitions_updated' LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION @extschema@.on_remove_partitions(relid OID)
RETURNS VOID AS 'pg_pathman', 'on_partitions_removed' LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION @extschema@.find_or_create_range_partition(relid OID, value ANYELEMENT)
RETURNS OID AS 'pg_pathman', 'find_or_create_range_partition' LANGUAGE C STRICT;


/*
 * Returns min and max values for specified RANGE partition.
 */
CREATE OR REPLACE FUNCTION @extschema@.get_partition_range(
    parent_relid OID, partition_relid OID, dummy ANYELEMENT)
RETURNS ANYARRAY AS 'pg_pathman', 'get_partition_range' LANGUAGE C STRICT;


/*
 * Returns N-th range (in form of array)
 */
CREATE OR REPLACE FUNCTION @extschema@.get_range_by_idx(
    parent_relid OID, idx INTEGER, dummy ANYELEMENT)
RETURNS ANYARRAY AS 'pg_pathman', 'get_range_by_idx' LANGUAGE C STRICT;


/*
 * Copy rows to partitions
 */
CREATE OR REPLACE FUNCTION @extschema@.partition_data(
    p_parent text
    , p_invalidate_cache_on_error BOOLEAN DEFAULT FALSE
    , OUT p_total BIGINT)
AS
$$
DECLARE
    rec RECORD;
    cnt BIGINT := 0;
BEGIN
    p_parent := @extschema@.validate_relname(p_parent);

    p_total := 0;

    /* Create partitions and copy rest of the data */
    RAISE NOTICE 'Copying data to partitions...';
    EXECUTE format('
                WITH part_data AS (
                    DELETE FROM ONLY %s RETURNING *)
                INSERT INTO %s SELECT * FROM part_data'
                , p_parent
                , p_parent);
    GET DIAGNOSTICS p_total = ROW_COUNT;
    -- RAISE NOTICE '% rows have been copied', p_total;
    RETURN;

EXCEPTION WHEN others THEN
    PERFORM on_remove_partitions(p_parent::regclass::integer);
    RAISE EXCEPTION '% %', SQLERRM, SQLSTATE;
END
$$
LANGUAGE plpgsql;


/*
 * Disable pathman partitioning for specified relation
 */
CREATE OR REPLACE FUNCTION @extschema@.disable_partitioning(IN relation TEXT)
RETURNS VOID AS
$$
BEGIN
    DELETE FROM @extschema@.pathman_config WHERE relname = relation;
    EXECUTE format('DROP FUNCTION IF EXISTS %s_insert_trigger_func() CASCADE', relation);

    /* Notify backend about changes */
    PERFORM on_remove_partitions(relation::regclass::integer);
END
$$
LANGUAGE plpgsql;


/*
 * Returns attribute type name for relation
 */
CREATE OR REPLACE FUNCTION @extschema@.get_attribute_type_name(
    p_relation TEXT
    , p_attname TEXT
    , OUT p_atttype TEXT)
RETURNS TEXT AS
$$
BEGIN
    SELECT typname::TEXT INTO p_atttype
    FROM pg_type JOIN pg_attribute on atttypid = "oid"
    WHERE attrelid = p_relation::regclass::oid and attname = lower(p_attname);
END
$$
LANGUAGE plpgsql;


/*
 * Validates relation name. It must be schema qualified
 */
CREATE OR REPLACE FUNCTION @extschema@.validate_relname(relname TEXT)
RETURNS TEXT AS
$$
BEGIN
    RETURN @extschema@.get_schema_qualified_name(relname::regclass, '.');
END
$$
LANGUAGE plpgsql;


/*
 * Returns schema-qualified name for table
 */
CREATE OR REPLACE FUNCTION @extschema@.get_schema_qualified_name(
    cls REGCLASS
    , delimiter TEXT DEFAULT '_')
RETURNS TEXT AS
$$
BEGIN
    RETURN relnamespace::regnamespace || delimiter || relname FROM pg_class WHERE oid = cls::oid;
END
$$
LANGUAGE plpgsql;

/*
 * DDL trigger that deletes entry from pathman_config
 */
CREATE OR REPLACE FUNCTION @extschema@.pathman_ddl_trigger_func()
RETURNS event_trigger AS
$$
DECLARE
    obj record;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_dropped_objects() as events
               JOIN @extschema@.pathman_config as cfg ON cfg.relname = events.object_identity
    LOOP
        IF obj.object_type = 'table' THEN
            EXECUTE 'DELETE FROM @extschema@.pathman_config WHERE relname = $1'
            USING obj.object_identity;
        END IF;
    END LOOP;
END
$$
LANGUAGE plpgsql;

CREATE EVENT TRIGGER pathman_ddl_trigger
ON sql_drop
EXECUTE PROCEDURE @extschema@.pathman_ddl_trigger_func();
