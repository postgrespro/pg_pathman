#include "postgres.h"

void pathman_get_fkeys(Oid parent_relid, List **constraints, List **indexes);
void createSingleForeignKeyTrigger(Oid relOid, Oid refRelOid, List *funcname,
								   char *trigname, int16 events, Oid constraintOid,
								   Oid indexOid);
