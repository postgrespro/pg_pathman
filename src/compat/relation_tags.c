/* ------------------------------------------------------------------------
 *
 * relation_tags.c
 *		Attach custom (Key, Value) pairs to an arbitrary RangeTblEntry
 *
 * Copyright (c) 2017, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "compat/relation_tags.h"
#include "planner_tree_modification.h"

#include "nodes/nodes.h"


#ifndef NATIVE_RELATION_TAGS

/*
 * This table is used to ensure that partitioned relation
 * cant't be referenced as ONLY and non-ONLY at the same time.
 */
static HTAB *per_table_relation_tags = NULL;

/*
 * Single row of 'per_table_relation_tags'.
 * NOTE: do not reorder these fields.
 */
typedef struct
{
	Oid			relid;		/* key (part #1) */
	uint32		queryId;	/* key (part #2) */
	List	   *relation_tags;
} relation_tags_entry;

#endif

/* Also used in get_refcount_relation_tags() etc... */
static int per_table_relation_tags_refcount = 0;



/* Look through RTE's relation tags */
List *
rte_fetch_tag(const uint32 query_id,
			  const RangeTblEntry *rte,
			  const char *key)
{
#ifdef NATIVE_RELATION_TAGS

	return relation_tags_search(rte->custom_tags, key);

#else

	relation_tags_entry	   *htab_entry,
							htab_key = { rte->relid, query_id, NIL /* unused */ };

	/* Skip if table is not initialized */
	if (per_table_relation_tags)
	{
		/* Search by 'htab_key' */
		htab_entry = hash_search(per_table_relation_tags,
								 &htab_key, HASH_FIND, NULL);

		if (htab_entry)
			return relation_tags_search(htab_entry->relation_tags, key);
	}

	/* Not found, return stub value */
	return NIL;

#endif
}

/* Attach new relation tag to RTE. Returns KVP with duplicate key. */
List *
rte_attach_tag(const uint32 query_id,
			   RangeTblEntry *rte,
			   List *key_value_pair)
{
	/* Common variables */
	MemoryContext			old_mcxt;
	const char			   *current_key;
	List				   *existing_kvp,
						   *temp_tags;		/* rte->custom_tags OR
											   htab_entry->relation_tags */

#ifdef NATIVE_RELATION_TAGS

	/* Load relation tags to 'temp_tags' */
	temp_tags = rte->custom_tags;

#else

	relation_tags_entry	   *htab_entry,
							htab_key = { rte->relid, query_id, NIL /* unused */ };
	bool					found;

	/* We prefer to initialize this table lazily */
	if (!per_table_relation_tags)
	{
		const long	start_elems = 50;
		HASHCTL		hashctl;

		memset(&hashctl, 0, sizeof(HASHCTL));
		hashctl.entrysize = sizeof(relation_tags_entry);
		hashctl.keysize = offsetof(relation_tags_entry, relation_tags);
		hashctl.hcxt = RELATION_TAG_MCXT;

		per_table_relation_tags = hash_create("Custom tags for RangeTblEntry",
											  start_elems, &hashctl,
											  HASH_ELEM | HASH_BLOBS);
	}

	/* Search by 'htab_key' */
	htab_entry = hash_search(per_table_relation_tags,
							 &htab_key, HASH_ENTER, &found);

	/* Don't forget to initialize list! */
	if (!found)
		htab_entry->relation_tags = NIL;

	/* Load relation tags to 'temp_tags' */
	temp_tags = htab_entry->relation_tags;

#endif

	/* Check that 'key_value_pair' is valid */
	AssertArg(key_value_pair && list_length(key_value_pair) == 2);

	/* Extract key of this KVP */
	rte_deconstruct_tag(key_value_pair, &current_key, NULL);

	/* Check if KVP with such key already exists */
	existing_kvp = relation_tags_search(temp_tags, current_key);
	if (existing_kvp)
		return existing_kvp; /* return KVP with duplicate key */

	/* Add this KVP to relation tags list */
	old_mcxt = MemoryContextSwitchTo(RELATION_TAG_MCXT);
	temp_tags = lappend(temp_tags, key_value_pair);
	MemoryContextSwitchTo(old_mcxt);

/* Finally store 'temp_tags' to relation tags list */
#ifdef NATIVE_RELATION_TAGS
	rte->custom_tags = temp_tags;
#else
	htab_entry->relation_tags = temp_tags;
#endif

	/* Success! */
	return NIL;
}



/* Extract key & value from 'key_value_pair' */
void
rte_deconstruct_tag(const List *key_value_pair,
					const char **key,		/* ret value #1 */
					const Value **value)	/* ret value #2 */
{
	const char	   *r_key;
	const Value	   *r_value;

	AssertArg(key_value_pair && list_length(key_value_pair) == 2);

	r_key = (const char *) strVal(linitial(key_value_pair));
	r_value = (const Value *) lsecond(key_value_pair);

	/* Check that 'key' is valid */
	Assert(IsA(linitial(key_value_pair), String));

	/* Check that 'value' is valid or NULL */
	Assert(r_value == NULL ||
		   IsA(r_value, Integer) ||
		   IsA(r_value, Float) ||
		   IsA(r_value, String));

	/* Finally return key & value */
	if (key) *key = r_key;
	if (value) *value = r_value;
}

/* Search through list of 'relation_tags' */
List *
relation_tags_search(List *relation_tags, const char *key)
{
	ListCell *lc;

	AssertArg(key);

	/* Scan KVP list */
	foreach (lc, relation_tags)
	{
		List	   *current_kvp = (List *) lfirst(lc);
		const char *current_key;

		/* Extract key of this KVP */
		rte_deconstruct_tag(current_kvp, &current_key, NULL);

		/* Check if this is the KVP we're looking for */
		if (strcmp(key, current_key) == 0)
			return current_kvp;
	}

	/* Nothing! */
	return NIL;
}



/* Increate usage counter by 1 */
void
incr_refcount_relation_tags(void)
{
	/* Increment reference counter */
	if (++per_table_relation_tags_refcount <= 0)
		elog(WARNING, "imbalanced %s",
			 CppAsString(incr_refcount_relation_tags));
}

/* Return current value of usage counter */
uint32
get_refcount_relation_tags(void)
{
	/* incr_refcount_parenthood_statuses() is called by pathman_planner_hook() */
	return per_table_relation_tags_refcount;
}

/* Reset all cached statuses if needed (query end) */
void
decr_refcount_relation_tags(void)
{
	/* Decrement reference counter */
	if (--per_table_relation_tags_refcount < 0)
		elog(WARNING, "imbalanced %s",
			 CppAsString(decr_refcount_relation_tags));

	/* Free resources if no one is using them */
	if (per_table_relation_tags_refcount == 0)
	{
		reset_query_id_generator();

#ifndef NATIVE_RELATION_TAGS
		hash_destroy(per_table_relation_tags);
		per_table_relation_tags = NULL;
#endif
	}
}
