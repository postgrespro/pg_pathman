/* ------------------------------------------------------------------------
 *
 * init.h
 *		Initialization functions
 *
 * Copyright (c) 2015-2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#ifndef PATHMAN_INIT_H
#define PATHMAN_INIT_H

#include "relation_info.h"

#include "postgres.h"
#include "storage/lmgr.h"
#include "utils/snapshot.h"
#include "utils/hsearch.h"


extern HTAB	   *partitioned_rels;
extern HTAB	   *parent_cache;
extern bool		initialization_needed;


Size estimate_pathman_shmem_size(void);
void init_shmem_config(void);
void load_config(void);
void unload_config(void);

void fill_prel_with_partitions(const Oid *partitions,
							   const uint32 parts_count,
							   PartRelationInfo *prel);

Oid *find_inheritance_children_array(Oid parentrelId,
									 LOCKMODE lockmode,
									 uint32 *size);

char *build_check_constraint_name_internal(Oid relid,
										   AttrNumber attno);

bool pathman_config_contains_relation(Oid relid,
									  Datum *values,
									  bool *isnull,
									  TransactionId *xmin);

#endif
