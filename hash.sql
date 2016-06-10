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
	relation REGCLASS
	, attribute TEXT
	, partitions_count INTEGER
) RETURNS INTEGER AS
$$
DECLARE
	v_relname       TEXT;
	v_child_relname TEXT;
	v_type          TEXT;
	v_plain_schema  TEXT;
	v_plain_relname TEXT;
	v_hashfunc      TEXT;
BEGIN
	v_relname := @extschema@.validate_relname(relation);
	attribute := lower(attribute);
	PERFORM @extschema@.common_relation_checks(relation, attribute);

	v_type := @extschema@.get_attribute_type_name(v_relname, attribute);
	-- IF v_type::regtype != 'integer'::regtype THEN
	-- 	RAISE EXCEPTION 'Attribute type must be INTEGER';
	-- END IF;

	SELECT * INTO v_plain_schema, v_plain_relname
	FROM @extschema@.get_plain_schema_and_relname(relation);

	v_hashfunc := @extschema@.get_type_hash_func(v_type::regtype::oid)::regproc;

	/* Create partitions and update pg_pathman configuration */
	FOR partnum IN 0..partitions_count-1
	LOOP
		-- v_child_relname := @extschema@.get_schema_qualified_name(relation, '.', suffix := '_' || partnum);
		v_child_relname := format('%s.%s',
        						  v_plain_schema,
        						  quote_ident(v_plain_relname || '_' || partnum));

		EXECUTE format('CREATE TABLE %s (LIKE %s INCLUDING ALL)'
						, v_child_relname
						, v_relname);

		EXECUTE format('ALTER TABLE %s INHERIT %s'
						, v_child_relname
						, v_relname);

		EXECUTE format('ALTER TABLE %s ADD CHECK (@extschema@.get_hash(%s(%s), %s) = %s)'
					   , v_child_relname
					   , v_hashfunc
					   , attribute
					   , partitions_count
					   , partnum);
	END LOOP;
	INSERT INTO @extschema@.pathman_config (relname, attname, parttype)
	VALUES (v_relname, attribute, 1);

	/* Create triggers */
	/* Do not create update trigger by default */
	-- PERFORM @extschema@.create_hash_update_trigger(relation, attribute, partitions_count);

	/* Notify backend about changes */
	PERFORM @extschema@.on_create_partitions(relation::oid);

	/* Copy data */
	PERFORM @extschema@.partition_data(relation);

	RETURN partitions_count;
END
$$ LANGUAGE plpgsql;

/*
 * Creates an update trigger
 */
CREATE OR REPLACE FUNCTION @extschema@.create_hash_update_trigger(
	IN relation REGCLASS)
RETURNS VOID AS
$$
DECLARE
	func TEXT := '
		CREATE OR REPLACE FUNCTION %s()
		RETURNS TRIGGER AS
		$body$
		DECLARE old_hash INTEGER; new_hash INTEGER; q TEXT;
		BEGIN
			old_hash := @extschema@.get_hash(%9$s(OLD.%2$s), %3$s);
			new_hash := @extschema@.get_hash(%9$s(NEW.%2$s), %3$s);
			IF old_hash = new_hash THEN RETURN NEW; END IF;
			q := format(''DELETE FROM %8$s WHERE %4$s'', old_hash);
			EXECUTE q USING %5$s;
			q := format(''INSERT INTO %8$s VALUES (%6$s)'', new_hash);
			EXECUTE q USING %7$s;
			RETURN NULL;
		END $body$ LANGUAGE plpgsql';
	trigger TEXT := '
		CREATE TRIGGER %s
		BEFORE UPDATE ON %s
		FOR EACH ROW EXECUTE PROCEDURE %s()';
	att_names     TEXT;
	old_fields    TEXT;
	new_fields    TEXT;
	att_val_fmt   TEXT;
	att_fmt       TEXT;
	relid         INTEGER;
	partitions_count INTEGER;
	attr          TEXT;
	plain_schema  TEXT;
	plain_relname TEXT;
	funcname      TEXT;
	triggername   TEXT;
	child_relname_format TEXT;
	atttype       TEXT;
	hashfunc      TEXT;
BEGIN
	relation := @extschema@.validate_relname(relation);

	SELECT * INTO plain_schema, plain_relname
	FROM @extschema@.get_plain_schema_and_relname(relation);

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

	attr := attname FROM @extschema@.pathman_config WHERE relname::regclass = relation;
	partitions_count := COUNT(*) FROM pg_inherits WHERE inhparent = relation::oid;

	/* Function name, trigger name and child relname template */
	funcname := plain_schema || '.' || quote_ident(format('%s_update_trigger_func', plain_relname));
	child_relname_format := plain_schema || '.' || quote_ident(plain_relname || '_%s');
	triggername := quote_ident(format('%s_%s_update_trigger', plain_schema, plain_relname));

	/* base hash function for type */
	atttype := @extschema@.get_attribute_type_name(relation, attr);
	hashfunc := @extschema@.get_type_hash_func(atttype::regtype::oid)::regproc;

	/* Format function definition and execute it */
	func := format(func, funcname, attr, partitions_count, att_val_fmt,
				   old_fields, att_fmt, new_fields, child_relname_format, hashfunc);
	EXECUTE func;

	/* Create triggers on child relations */
	FOR num IN 0..partitions_count-1
	LOOP
		EXECUTE format(trigger
					   , triggername
					   , format(child_relname_format, num)
					   , funcname);
	END LOOP;
END
$$ LANGUAGE plpgsql;
