/* ------------------------------------------------------------------------
 *
 * relation_tags.h
 *		Attach custom (Key, Value) pairs to an arbitrary RangeTblEntry
 *
 *		NOTE: implementations for vanilla and PostgresPro differ,
 *		which means that subquery pull-up might break the bond
 *		between a RangeTblEntry and the corresponding KVPs.
 *
 *		This subsystem was meant to replace the broken 'inh' flag
 *		(see get_rel_parenthood_status() for more details).
 *
 * Copyright (c) 2017, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#ifndef RELATION_TAGS_H
#define RELATION_TAGS_H

#include "compat/debug_compat_features.h"

#include "postgres.h"
#include "nodes/relation.h"
#include "nodes/value.h"
#include "utils/memutils.h"


/* Does RTE contain 'custom_tags' list? */
/* TODO: fix this definition once PgPro contains 'relation_tags' patch */
#if defined(ENABLE_PGPRO_PATCHES) && \
	defined(ENABLE_RELATION_TAGS) /* && ... */
#define NATIVE_RELATION_TAGS
#endif

/* Memory context we're going to use for tags */
#define RELATION_TAG_MCXT TopTransactionContext

extern void MarkPartitionedRTE(Index rti);
extern bool IsPartitionedRTE(Index rti);

/* Safe TAG constructor (Integer) */
static inline List *
make_rte_tag_int(char *key, int value)
{
	List		   *kvp;
	MemoryContext	old_mcxt;

	/* Allocate TAG in a persistent memory context */
	old_mcxt = MemoryContextSwitchTo(RELATION_TAG_MCXT);
	kvp = list_make2(makeString(key), makeInteger(value));
	MemoryContextSwitchTo(old_mcxt);

	return kvp;
}


List *rte_fetch_tag(const uint32 query_id,
					const RangeTblEntry *rte,
					const char *key);

List *rte_attach_tag(const uint32 query_id,
					 RangeTblEntry *rte,
					 List *key_value_pair);


List *relation_tags_search(List *custom_tags,
						   const char *key);

void rte_deconstruct_tag(const List *key_value_pair,
						 const char **key,
						 const Value **value);


void incr_refcount_relation_tags(void);
uint32 get_refcount_relation_tags(void);
void decr_refcount_relation_tags(void);

#endif /* RELATION_TAGS_H */
