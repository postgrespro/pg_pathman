#include "postgres.h"

void pathman_get_fkeys(Oid parent_relid, List **constraints, List **refrelids);
void createSingleForeignKeyTrigger(Oid relOid, Oid refRelOid, List *funcname,
								   char *trigname, int16 events, Oid constraintOid,
								   Oid indexOid);
void createPartitionForeignKeyTriggers(Oid partition,
									   Oid fkrelid,
									   AttrNumber attnum,
									   Oid constraintOid,
									   List *upd_funcname,
									   List *del_funcname);