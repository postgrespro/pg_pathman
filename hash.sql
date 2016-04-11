/* ------------------------------------------------------------------------
 *
 * hash.sql
 *      HASH partitioning functions
 *
 * Copyright (c) 2015-2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

/*
 * Creates hash partitions for specified relation
 */
CREATE OR REPLACE FUNCTION @extschema@.create_hash_partitions(
	relation TEXT
	, attribute TEXT
	, partitions_count INTEGER
) RETURNS INTEGER AS
$$
DECLARE
	v_type TEXT;
BEGIN
	relation := @extschema@.validate_relname(relation);
	attribute := lower(attribute);
	PERFORM @extschema@.common_relation_checks(relation, attribute);

	v_type := @extschema@.get_attribute_type_name(relation, attribute);
	IF v_type::regtype != 'integer'::regtype THEN
		RAISE EXCEPTION 'Attribute type must be INTEGER';
	END IF;

	/* Create partitions and update pg_pathman configuration */
	FOR partnum IN 0..partitions_count-1
	LOOP
		EXECUTE format('CREATE TABLE %s_%s (LIKE %1$s INCLUDING ALL)'
						, relation
						, partnum);

		EXECUTE format('ALTER TABLE %s_%s INHERIT %1$s'
						, relation
						, partnum);

		EXECUTE format('ALTER TABLE %s_%s ADD CHECK (%s %% %s = %s)'
					   , relation
					   , partnum
					   , attribute
					   , partitions_count
					   , partnum);
	END LOOP;
	INSERT INTO @extschema@.pathman_config (relname, attname, parttype)
	VALUES (relation, attribute, 1);

	/* Create triggers */
	PERFORM @extschema@.create_hash_insert_trigger(relation, attribute, partitions_count);
	/* Do not create update trigger by default */
	-- PERFORM @extschema@.create_hash_update_trigger(relation, attribute, partitions_count);

	/* Notify backend about changes */
	PERFORM @extschema@.on_create_partitions(relation::regclass::oid);

	/* Copy data */
	PERFORM @extschema@.partition_data(relation);

	RETURN partitions_count;
END
$$ LANGUAGE plpgsql;

/*
 * Creates hash trigger for specified relation
 */
CREATE OR REPLACE FUNCTION @extschema@.create_hash_insert_trigger(
	IN relation TEXT
	, IN attr TEXT
	, IN partitions_count INTEGER)
RETURNS VOID AS
$$
DECLARE
	func TEXT := '
		CREATE OR REPLACE FUNCTION %s_insert_trigger_func()
		RETURNS TRIGGER AS $body$
		DECLARE
			hash INTEGER;
		BEGIN
			hash := NEW.%s %% %s;
			%s
			RETURN NULL;
		END $body$ LANGUAGE plpgsql;';
	trigger TEXT := '
		CREATE TRIGGER %s_insert_trigger
		BEFORE INSERT ON %s
		FOR EACH ROW EXECUTE PROCEDURE %2$s_insert_trigger_func();';
	fields TEXT;
	fields_format TEXT;
	insert_stmt TEXT;
BEGIN
	/* drop trigger and corresponding function */
	PERFORM @extschema@.drop_hash_triggers(relation);

	/* determine fields for INSERT */
	SELECT string_agg('NEW.' || attname, ', '), string_agg('$' || attnum, ', ')
	FROM pg_attribute
	WHERE attrelid=relation::regclass::oid AND attnum>0
	INTO fields, fields_format;

	/* generate INSERT statement for trigger */
	insert_stmt = format('EXECUTE format(''INSERT INTO %s_%%s SELECT $1.*'', hash) USING NEW;'
						 , relation);

	/* format and create new trigger for relation */
	func := format(func, relation, attr, partitions_count, insert_stmt);
	trigger := format(trigger, @extschema@.get_schema_qualified_name(relation::regclass), relation);
	EXECUTE func;
	EXECUTE trigger;
END
$$ LANGUAGE plpgsql;

/*
 * Drops all partitions for specified relation
 */
CREATE OR REPLACE FUNCTION @extschema@.drop_hash_partitions(
	IN relation TEXT
	, delete_data BOOLEAN DEFAULT FALSE)
RETURNS INTEGER AS
$$
DECLARE
	v_rec RECORD;
	v_rows INTEGER;
	v_part_count INTEGER := 0;
BEGIN
	relation := @extschema@.validate_relname(relation);

	/* Drop trigger first */
	PERFORM @extschema@.drop_hash_triggers(relation);
	DELETE FROM @extschema@.pathman_config WHERE relname = relation;

	FOR v_rec in (SELECT inhrelid::regclass::text AS tbl
				FROM pg_inherits WHERE inhparent = relation::regclass::oid)
	LOOP
		IF NOT delete_data THEN
			EXECUTE format('WITH part_data AS (DELETE FROM %s RETURNING *)
							INSERT INTO %s SELECT * FROM part_data'
						   , v_rec.tbl
						   , relation);
			GET DIAGNOSTICS v_rows = ROW_COUNT;
			RAISE NOTICE '% rows copied from %', v_rows, v_rec.tbl;
		END IF;
		EXECUTE format('DROP TABLE %s', v_rec.tbl);
		v_part_count := v_part_count + 1;
	END LOOP;

	/* Notify backend about changes */
	PERFORM @extschema@.on_remove_partitions(relation::regclass::oid);

	RETURN v_part_count;
END
$$ LANGUAGE plpgsql;

/*
 * Drops hash trigger
 */
CREATE OR REPLACE FUNCTION @extschema@.drop_hash_triggers(IN relation TEXT)
RETURNS VOID AS
$$
BEGIN
	EXECUTE format('DROP FUNCTION IF EXISTS %s_insert_trigger_func() CASCADE'
				   , relation::regclass::text);
	EXECUTE format('DROP FUNCTION IF EXISTS %s_update_trigger_func() CASCADE'
				   , relation::regclass::text);
END
$$ LANGUAGE plpgsql;

/*
 * Creates an update trigger
 */
CREATE OR REPLACE FUNCTION @extschema@.create_hash_update_trigger(
	IN relation TEXT)
RETURNS VOID AS
$$
DECLARE
	func TEXT := '
		CREATE OR REPLACE FUNCTION %s_update_trigger_func()
		RETURNS TRIGGER AS
		$body$
		DECLARE old_hash INTEGER; new_hash INTEGER; q TEXT;
		BEGIN
			old_hash := OLD.%2$s %% %3$s;
			new_hash := NEW.%2$s %% %3$s;
			IF old_hash = new_hash THEN RETURN NEW; END IF;
			q := format(''DELETE FROM %1$s_%%s WHERE %4$s'', old_hash);
			EXECUTE q USING %5$s;
			q := format(''INSERT INTO %1$s_%%s VALUES (%6$s)'', new_hash);
			EXECUTE q USING %7$s;
			RETURN NULL;
		END $body$ LANGUAGE plpgsql';
	trigger TEXT := 'CREATE TRIGGER %s_%s_update_trigger ' ||  
		'BEFORE UPDATE ON %s_%2$s ' ||
		'FOR EACH ROW EXECUTE PROCEDURE %3$s_update_trigger_func()';
	att_names   TEXT;
	old_fields  TEXT;
	new_fields  TEXT;
	att_val_fmt TEXT;
	att_fmt     TEXT;
	relid       INTEGER;
	partitions_count INTEGER;
	attr        TEXT;
BEGIN
	relation := @extschema@.validate_relname(relation);
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
	partitions_count := COUNT(*) FROM pg_inherits WHERE inhparent = relation::regclass::oid;
	EXECUTE format(func, relation, attr, partitions_count, att_val_fmt,
				   old_fields, att_fmt, new_fields);
	FOR num IN 0..partitions_count-1
	LOOP
		EXECUTE format(trigger
					   , @extschema@.get_schema_qualified_name(relation::regclass)
					   , num
					   , relation);
	END LOOP;
END
$$ LANGUAGE plpgsql;
