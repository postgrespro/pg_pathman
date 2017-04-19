/* ------------------------------------------------------------------------
 *
 * ref_integrity.c
 *		Referential integrity for partitioned tables
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "postgres.h"


void pathman_get_fkeys(Oid parent_relid, List **constraints, List **refrelids);
void createPartitionForeignKeyTriggers(Oid partition,
								  Oid fkrelid,
								  AttrNumber attnum,
								  Oid constraintOid,
								  List *upd_funcname,
								  List *del_funcname);
void createSingleForeignKeyTrigger(Oid relOid, Oid refRelOid, List *funcname,
							  char *trigname, int16 events, Oid constraintOid,
							  Oid indexOid, bool is_internal);
HeapTuple get_index_for_key(Relation rel, AttrNumber attnum, Oid *index_id);
void ri_checkReferences(Relation partition, Oid constraintOid);
List * get_ri_triggers_list(Oid relid, Oid constr);
void ri_preparePartitionDrop(Oid parent,
						Relation partition,
						bool check_references);
void enable_ri_triggers(void);
void disable_ri_triggers(void);
