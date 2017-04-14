#include "postgres.h"

void pathman_get_fkeys(Oid parent_relid, List **constraints, List **refrelids);
void createSingleForeignKeyTrigger(Oid relOid, Oid refRelOid, List *funcname,
								   char *trigname, int16 events, Oid constraintOid,
								   Oid indexOid, bool is_internal);
void createPartitionForeignKeyTriggers(Oid partition,
									   Oid fkrelid,
									   AttrNumber attnum,
									   Oid constraintOid,
									   List *upd_funcname,
									   List *del_funcname);
HeapTuple get_index_for_key(Relation rel, AttrNumber attnum, Oid *index_id);
List *get_ri_triggers_list(Oid relid, Oid constr);
void ri_removePartitionDependencies(Oid parent, Relation partition);
void ri_checkReferences(Relation partition, Oid constraintOid);