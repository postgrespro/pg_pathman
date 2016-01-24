/*
 * Creates RANGE partitions for specified relation based on datetime attribute
 */
CREATE OR REPLACE FUNCTION @extschema@.create_range_partitions(
    p_relation TEXT
    , p_attribute TEXT
    , p_start_value ANYELEMENT
    , p_interval INTERVAL
    , p_premake INTEGER)
RETURNS VOID AS
$$
DECLARE
    v_value     TEXT;
    i INTEGER;
BEGIN
    p_relation := @extschema@.validate_relname(p_relation);

    IF EXISTS (SELECT * FROM @extschema@.pathman_config WHERE relname = p_relation) THEN
        RAISE EXCEPTION 'Reltion "%" has already been partitioned', p_relation;
    END IF;

    EXECUTE format('DROP SEQUENCE IF EXISTS %s_seq', p_relation);
    EXECUTE format('CREATE SEQUENCE %s_seq START 1', p_relation);

    INSERT INTO @extschema@.pathman_config (relname, attname, parttype)
    VALUES (p_relation, p_attribute, 2);

    /* create first partition */
    FOR i IN 1..p_premake+1
    LOOP
        EXECUTE format('SELECT @extschema@.create_single_range_partition($1, $2, $3::%s);', pg_typeof(p_start_value))
        USING p_relation, p_start_value, p_start_value + p_interval;

        p_start_value := p_start_value + p_interval;
    END LOOP;

    /* Create triggers */
    PERFORM @extschema@.create_range_insert_trigger(p_relation, p_attribute);
    -- PERFORM create_hash_update_trigger(relation, attribute, partitions_count);
    /* Notify backend about changes */
    PERFORM @extschema@.on_create_partitions(p_relation::regclass::oid);
END
$$ LANGUAGE plpgsql;

/*
 * Creates RANGE partitions for specified relation based on numerical attribute
 */
CREATE OR REPLACE FUNCTION @extschema@.create_range_partitions(
    p_relation TEXT
    , p_attribute TEXT
    , p_start_value ANYELEMENT
    , p_interval ANYELEMENT
    , p_premake INTEGER)
RETURNS VOID AS
$$
DECLARE
    v_value     TEXT;
    i INTEGER;
BEGIN
    p_relation := @extschema@.validate_relname(p_relation);

    IF EXISTS (SELECT * FROM @extschema@.pathman_config WHERE relname = p_relation) THEN
        RAISE EXCEPTION 'Reltion "%" has already been partitioned', p_relation;
    END IF;

    EXECUTE format('DROP SEQUENCE IF EXISTS %s_seq', p_relation);
    EXECUTE format('CREATE SEQUENCE %s_seq START 1', p_relation);

    INSERT INTO @extschema@.pathman_config (relname, attname, parttype)
    VALUES (p_relation, p_attribute, 2);

    /* create first partition */
    FOR i IN 1..p_premake+1
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
    p_parent_relname TEXT
    , p_start_value  ANYELEMENT
    , p_end_value    ANYELEMENT)
RETURNS TEXT AS
$$
DECLARE
    v_child_relname TEXT;
    v_attname TEXT;

    v_part_num INT;
    v_sql TEXT;
    -- v_type TEXT;
    v_cond TEXT;
BEGIN
    v_attname := attname FROM @extschema@.pathman_config
                 WHERE relname = p_parent_relname;

    /* get next value from sequence */
    v_part_num := nextval(format('%s_seq', p_parent_relname));
    v_child_relname := format('%s_%s', p_parent_relname, v_part_num);

    /* Skip existing partitions */
    IF EXISTS (SELECT * FROM pg_tables WHERE tablename = v_child_relname) THEN
        RAISE WARNING 'Relation % already exists, skipping...', v_child_relname;
        RETURN NULL;
    END IF;

    EXECUTE format('CREATE TABLE %s (LIKE %s INCLUDING ALL)'
                   , v_child_relname
                   , p_parent_relname);

    EXECUTE format('ALTER TABLE %s INHERIT %s'
                   , v_child_relname
                   , p_parent_relname);

    v_cond := @extschema@.get_range_condition(v_attname, p_start_value, p_end_value);
    v_sql := format('ALTER TABLE %s ADD CONSTRAINT %s_check CHECK (%s)'
                    , v_child_relname
                    , @extschema@.get_schema_qualified_name(v_child_relname::regclass)
                    , v_cond);

    EXECUTE v_sql;
    RETURN v_child_relname;
END
$$ LANGUAGE plpgsql;


/*
 * Split RANGE partition
 */
CREATE OR REPLACE FUNCTION @extschema@.split_range_partition(
    p_partition TEXT
    , p_value ANYELEMENT
    , OUT p_range ANYARRAY)
RETURNS ANYARRAY AS
$$
DECLARE
    v_parent_relid OID;
    v_child_relid OID := p_partition::regclass::oid;
    v_attname TEXT;
    v_cond TEXT;
    v_new_partition TEXT;
    v_part_type INTEGER;
BEGIN
    p_partition := @extschema@.validate_relname(p_partition);

    v_parent_relid := inhparent
                      FROM pg_inherits
                      WHERE inhrelid = v_child_relid;

    SELECT attname, parttype INTO v_attname, v_part_type
    FROM @extschema@.pathman_config
    WHERE relname = v_parent_relid::regclass::text;

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
    v_new_partition := @extschema@.create_single_range_partition(v_parent_relid::regclass::text,
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
    EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %s_check'
                   , p_partition
                   , @extschema@.get_schema_qualified_name(p_partition::regclass));
    EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %s_check CHECK (%s)'
                   , p_partition
                   , @extschema@.get_schema_qualified_name(p_partition::regclass)
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
    p_partition1 TEXT
    , p_partition2 TEXT)
RETURNS VOID AS
$$
DECLARE
    v_parent_relid1 OID;
    v_parent_relid2 OID;
    v_part1_relid OID := p_partition1::regclass::oid;
    v_part2_relid OID := p_partition2::regclass::oid;
    v_attname TEXT;
    v_part_type INTEGER;
    v_atttype TEXT;
BEGIN
    p_partition1 := @extschema@.validate_relname(p_partition1);
    p_partition2 := @extschema@.validate_relname(p_partition2);

    IF v_part1_relid = v_part2_relid THEN
        RAISE EXCEPTION 'Cannot merge partition to itself';
    END IF;

    v_parent_relid1 := inhparent FROM pg_inherits WHERE inhrelid = v_part1_relid;
    v_parent_relid2 := inhparent FROM pg_inherits WHERE inhrelid = v_part2_relid;

    IF v_parent_relid1 != v_parent_relid2 THEN
        RAISE EXCEPTION 'Cannot merge partitions having different parents';
    END IF;

    SELECT attname, parttype INTO v_attname, v_part_type
    FROM @extschema@.pathman_config
    WHERE relname = v_parent_relid1::regclass::text;

    /* Check if this is RANGE partition */
    IF v_part_type != 2 THEN
        RAISE EXCEPTION 'Specified partitions aren''t RANGE partitions';
    END IF;

    v_atttype := @extschema@.get_attribute_type_name(v_parent_relid1::regclass::text, v_attname);

    EXECUTE format('SELECT @extschema@.merge_range_partitions_internal($1, $2 , $3, NULL::%s)', v_atttype)
    USING v_parent_relid1, v_part1_relid , v_part2_relid;

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
 * (it is neccessary because of pseudo-types used in function)
 */
CREATE OR REPLACE FUNCTION @extschema@.merge_range_partitions_internal(
    p_parent_relid OID
    , p_part1_relid OID
    , p_part2_relid OID
    , dummy ANYELEMENT
    , OUT p_range ANYARRAY)
RETURNS ANYARRAY AS
$$
DECLARE
    v_attname TEXT;
    v_cond TEXT;
BEGIN
    SELECT attname INTO v_attname FROM @extschema@.pathman_config
    WHERE relname = p_parent_relid::regclass::text;

    /*
     * Get ranges
     * first and second elements of array are MIN and MAX of partition1
     * third and forth elements are MIN and MAX of partition2
     */
    p_range := @extschema@.get_partition_range(p_parent_relid, p_part1_relid, 0) ||
               @extschema@.get_partition_range(p_parent_relid, p_part2_relid, 0);

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
    EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %s_check'
                   , p_part1_relid::regclass::text
                   , @extschema@.get_schema_qualified_name(p_part1_relid::regclass));
    EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %s_check CHECK (%s)'
                   , p_part1_relid::regclass::text
                   , @extschema@.get_schema_qualified_name(p_part1_relid::regclass)
                   , v_cond);

    /* Copy data from second partition to the first one */
    RAISE NOTICE 'Copying data...';
    EXECUTE format('WITH part_data AS (DELETE FROM %s RETURNING *)
                    INSERT INTO %s SELECT * FROM part_data'
                   , p_part2_relid::regclass::text
                   , p_part1_relid::regclass::text);

    /* Remove second partition */
    RAISE NOTICE 'Dropping second partition...';
    EXECUTE format('DROP TABLE %s', p_part2_relid::regclass::text);
END
$$ LANGUAGE plpgsql;


/*
 * Append new partition
 */
CREATE OR REPLACE FUNCTION @extschema@.append_partition(p_relation TEXT)
RETURNS VOID AS
$$
DECLARE
    v_attname TEXT;
    v_atttype TEXT;
BEGIN
    p_relation := @extschema@.validate_relname(p_relation);

    v_attname := attname FROM @extschema@.pathman_config WHERE relname = p_relation;
    v_atttype := @extschema@.get_attribute_type_name(p_relation, v_attname);
    EXECUTE format('SELECT @extschema@.append_partition_internal($1, NULL::%s)', v_atttype)
    USING p_relation;
END
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION @extschema@.append_partition_internal(
    p_relation TEXT
    , dummy ANYELEMENT
    , OUT p_range ANYARRAY)
RETURNS ANYARRAY AS
$$
BEGIN
    p_range := @extschema@.get_range_by_idx(p_relation::regclass::oid, -1, 0);
    RAISE NOTICE 'Appending new partition...';
    PERFORM @extschema@.create_single_range_partition(p_relation
                                                      , p_range[2]
                                                      , p_range[2] + (p_range[2] - p_range[1]));

    /* Tell backend to reload configuration */
    PERFORM @extschema@.on_create_partitions(p_relation::regclass::oid);
    RAISE NOTICE 'Done!';
END
$$
LANGUAGE plpgsql;


/*
 * Append new partition
 */
CREATE OR REPLACE FUNCTION @extschema@.prepend_partition(p_relation TEXT)
RETURNS VOID AS
$$
DECLARE
    v_attname TEXT;
    v_atttype TEXT;
BEGIN
    p_relation := @extschema@.validate_relname(p_relation);

    v_attname := attname FROM @extschema@.pathman_config WHERE relname = p_relation;
    v_atttype := @extschema@.get_attribute_type_name(p_relation, v_attname);
    EXECUTE format('SELECT @extschema@.prepend_partition_internal($1, NULL::%s)', v_atttype)
    USING p_relation;
END
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION @extschema@.prepend_partition_internal(
    p_relation TEXT
    , dummy ANYELEMENT
    , OUT p_range ANYARRAY)
RETURNS ANYARRAY AS
$$
BEGIN
    p_range := @extschema@.get_range_by_idx(p_relation::regclass::oid, 0, 0);
    RAISE NOTICE 'Prepending new partition...';
    PERFORM @extschema@.create_single_range_partition(p_relation
                                                      , p_range[1] - (p_range[2] - p_range[1])
                                                      , p_range[1]);

    /* Tell backend to reload configuration */
    PERFORM @extschema@.on_create_partitions(p_relation::regclass::oid);
    RAISE NOTICE 'Done!';
END
$$
LANGUAGE plpgsql;


/*
 * Creates range partitioning insert trigger
 */
CREATE OR REPLACE FUNCTION @extschema@.create_range_insert_trigger(
    v_relation    TEXT
    , v_attname   TEXT)
RETURNS VOID AS
$$
DECLARE
    v_func TEXT := '
        CREATE OR REPLACE FUNCTION %s_insert_trigger_func()
        RETURNS TRIGGER
        AS $body$
        DECLARE
            v_part_relid OID;
        BEGIN
            IF TG_OP = ''INSERT'' THEN
                v_part_relid := @extschema@.find_range_partition(TG_RELID, NEW.%s);
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
    v_trigger TEXT := '
        CREATE TRIGGER %s_insert_trigger
        BEFORE INSERT ON %s
        FOR EACH ROW EXECUTE PROCEDURE %2$s_insert_trigger_func();';
BEGIN
    v_func := format(v_func, v_relation, v_attname);
    v_trigger := format(v_trigger, @extschema@.get_schema_qualified_name(v_relation::regclass), v_relation);

    EXECUTE v_func;
    EXECUTE v_trigger;
    RETURN;
END
$$ LANGUAGE plpgsql;


/*
 * Creates an update trigger
 */
CREATE OR REPLACE FUNCTION @extschema@.create_range_update_trigger(
    IN relation TEXT)
RETURNS VOID AS
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
            new_oid := @extschema@.find_range_partition(''%1$s''::regclass::oid, NEW.%2$s);
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
    relid := relation::regclass::oid;
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

    attr := attname FROM @extschema@.pathman_config WHERE relname = relation;
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
END
$$ LANGUAGE plpgsql;


/*
 * Drop partitions
 */
CREATE OR REPLACE FUNCTION @extschema@.drop_range_partitions(IN relation TEXT)
RETURNS VOID AS
$$
DECLARE
    v_rec   RECORD;
BEGIN
    relation := @extschema@.validate_relname(relation);

    /* Drop trigger first */
    PERFORM @extschema@.drop_range_triggers(relation);

    FOR v_rec IN (SELECT inhrelid::regclass AS tbl
                  FROM pg_inherits WHERE inhparent = relation::regclass::oid)
    LOOP
        EXECUTE format('DROP TABLE %s', v_rec.tbl);
    END LOOP;

    DELETE FROM @extschema@.pathman_config WHERE relname = relation;
    -- DELETE FROM pg_pathman_range_rels WHERE parent = relation;

    /* Notify backend about changes */
    PERFORM @extschema@.on_remove_partitions(relation::regclass::oid);
END
$$ LANGUAGE plpgsql;


/*
 * Drop trigger
 */
CREATE OR REPLACE FUNCTION @extschema@.drop_range_triggers(IN relation TEXT)
RETURNS VOID AS
$$
BEGIN
    EXECUTE format('DROP TRIGGER IF EXISTS %s_insert_trigger ON %s CASCADE'
                   , @extschema@.get_schema_qualified_name(relation::regclass)
                   , relation);
END
$$ LANGUAGE plpgsql;
