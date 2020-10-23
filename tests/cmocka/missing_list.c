/*-------------------------------------------------------------------------
 *
 * list.c
 *	  implementation for PostgreSQL generic list package
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/nodes/list.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "nodes/pg_list.h"

#if PG_VERSION_NUM < 130000

#define IsPointerList(l)		((l) == NIL || IsA((l), List))
#define IsIntegerList(l)		((l) == NIL || IsA((l), IntList))
#define IsOidList(l)			((l) == NIL || IsA((l), OidList))


static List *
new_list(NodeTag type);

static void
new_tail_cell(List *list);

static void
new_head_cell(List *list);

static void
check_list_invariants(const List *list);


/*
 * -------------
 *  Definitions
 * -------------
 */

static List *
new_list(NodeTag type)
{
	List	   *new_list;
	ListCell   *new_head;

	new_head = (ListCell *) palloc(sizeof(*new_head));
	new_head->next = NULL;
	/* new_head->data is left undefined! */

	new_list = (List *) palloc(sizeof(*new_list));
	new_list->type = type;
	new_list->length = 1;
	new_list->head = new_head;
	new_list->tail = new_head;

	return new_list;

}

static void
new_tail_cell(List *list)
{
	ListCell   *new_tail;

	new_tail = (ListCell *) palloc(sizeof(*new_tail));
	new_tail->next = NULL;

	list->tail->next = new_tail;
	list->tail = new_tail;
	list->length++;

}

static void
new_head_cell(List *list)
{
	ListCell   *new_head;

	new_head = (ListCell *) palloc(sizeof(*new_head));
	new_head->next = list->head;

	list->head = new_head;
	list->length++;

}

static void
check_list_invariants(const List *list)
{
	if (list == NIL)
		return;

	Assert(list->length > 0);
	Assert(list->head != NULL);
	Assert(list->tail != NULL);

	Assert(list->type == T_List ||
			list->type == T_IntList ||
			list->type == T_OidList);

	if (list->length == 1)
		Assert(list->head == list->tail);
	if (list->length == 2)
		Assert(list->head->next == list->tail);
	Assert(list->tail->next == NULL);

}

List *
lappend(List *list, void *datum)
{
	Assert(IsPointerList(list));

	if (list == NIL)
		list = new_list(T_List);
	else
		new_tail_cell(list);

	lfirst(list->tail) = datum;
	check_list_invariants(list);
	return list;
}

List *
lcons(void *datum, List *list)
{
	Assert(IsPointerList(list));

	if (list == NIL)
		list = new_list(T_List);
	else
		new_head_cell(list);

	lfirst(list->head) = datum;
	check_list_invariants(list);
	return list;

}

#else /* PG_VERSION_NUM >= 130000 */

/*-------------------------------------------------------------------------
 *
 * This was taken from src/backend/nodes/list.c PostgreSQL-13 source code.
 * We only need lappend() and lcons() and their dependencies.
 * There is one change: we use palloc() instead MemoryContextAlloc() in
 * enlarge_list() (see #defines).
 *
 *-------------------------------------------------------------------------
 */
#include "port/pg_bitutils.h"
#include "utils/memdebug.h"
#include "utils/memutils.h"

#define MemoryContextAlloc(c, s) palloc(s)
#define GetMemoryChunkContext(l) 0

/*
 * The previous List implementation, since it used a separate palloc chunk
 * for each cons cell, had the property that adding or deleting list cells
 * did not move the storage of other existing cells in the list.  Quite a
 * bit of existing code depended on that, by retaining ListCell pointers
 * across such operations on a list.  There is no such guarantee in this
 * implementation, so instead we have debugging support that is meant to
 * help flush out now-broken assumptions.  Defining DEBUG_LIST_MEMORY_USAGE
 * while building this file causes the List operations to forcibly move
 * all cells in a list whenever a cell is added or deleted.  In combination
 * with MEMORY_CONTEXT_CHECKING and/or Valgrind, this can usually expose
 * broken code.  It's a bit expensive though, as there's many more palloc
 * cycles and a lot more data-copying than in a default build.
 *
 * By default, we enable this when building for Valgrind.
 */
#ifdef USE_VALGRIND
#define DEBUG_LIST_MEMORY_USAGE
#endif

/* Overhead for the fixed part of a List header, measured in ListCells */
#define LIST_HEADER_OVERHEAD  \
	((int) ((offsetof(List, initial_elements) - 1) / sizeof(ListCell) + 1))

/*
 * Macros to simplify writing assertions about the type of a list; a
 * NIL list is considered to be an empty list of any type.
 */
#define IsPointerList(l)		((l) == NIL || IsA((l), List))
#define IsIntegerList(l)		((l) == NIL || IsA((l), IntList))
#define IsOidList(l)			((l) == NIL || IsA((l), OidList))

#ifdef USE_ASSERT_CHECKING
/*
 * Check that the specified List is valid (so far as we can tell).
 */
static void
check_list_invariants(const List *list)
{
	if (list == NIL)
		return;

	Assert(list->length > 0);
	Assert(list->length <= list->max_length);
	Assert(list->elements != NULL);

	Assert(list->type == T_List ||
		   list->type == T_IntList ||
		   list->type == T_OidList);
}
#else
#define check_list_invariants(l)  ((void) 0)
#endif							/* USE_ASSERT_CHECKING */

/*
 * Return a freshly allocated List with room for at least min_size cells.
 *
 * Since empty non-NIL lists are invalid, new_list() sets the initial length
 * to min_size, effectively marking that number of cells as valid; the caller
 * is responsible for filling in their data.
 */
static List *
new_list(NodeTag type, int min_size)
{
	List	   *newlist;
	int			max_size;

	Assert(min_size > 0);

	/*
	 * We allocate all the requested cells, and possibly some more, as part of
	 * the same palloc request as the List header.  This is a big win for the
	 * typical case of short fixed-length lists.  It can lose if we allocate a
	 * moderately long list and then it gets extended; we'll be wasting more
	 * initial_elements[] space than if we'd made the header small.  However,
	 * rounding up the request as we do in the normal code path provides some
	 * defense against small extensions.
	 */

#ifndef DEBUG_LIST_MEMORY_USAGE

	/*
	 * Normally, we set up a list with some extra cells, to allow it to grow
	 * without a repalloc.  Prefer cell counts chosen to make the total
	 * allocation a power-of-2, since palloc would round it up to that anyway.
	 * (That stops being true for very large allocations, but very long lists
	 * are infrequent, so it doesn't seem worth special logic for such cases.)
	 *
	 * The minimum allocation is 8 ListCell units, providing either 4 or 5
	 * available ListCells depending on the machine's word width.  Counting
	 * palloc's overhead, this uses the same amount of space as a one-cell
	 * list did in the old implementation, and less space for any longer list.
	 *
	 * We needn't worry about integer overflow; no caller passes min_size
	 * that's more than twice the size of an existing list, so the size limits
	 * within palloc will ensure that we don't overflow here.
	 */
	max_size = pg_nextpower2_32(Max(8, min_size + LIST_HEADER_OVERHEAD));
	max_size -= LIST_HEADER_OVERHEAD;
#else

	/*
	 * For debugging, don't allow any extra space.  This forces any cell
	 * addition to go through enlarge_list() and thus move the existing data.
	 */
	max_size = min_size;
#endif

	newlist = (List *) palloc(offsetof(List, initial_elements) +
							  max_size * sizeof(ListCell));
	newlist->type = type;
	newlist->length = min_size;
	newlist->max_length = max_size;
	newlist->elements = newlist->initial_elements;

	return newlist;
}

/*
 * Enlarge an existing non-NIL List to have room for at least min_size cells.
 *
 * This does *not* update list->length, as some callers would find that
 * inconvenient.  (list->length had better be the correct number of existing
 * valid cells, though.)
 */
static void
enlarge_list(List *list, int min_size)
{
	int			new_max_len;

	Assert(min_size > list->max_length);	/* else we shouldn't be here */

#ifndef DEBUG_LIST_MEMORY_USAGE

	/*
	 * As above, we prefer power-of-two total allocations; but here we need
	 * not account for list header overhead.
	 */

	/* clamp the minimum value to 16, a semi-arbitrary small power of 2 */
	new_max_len = pg_nextpower2_32(Max(16, min_size));

#else
	/* As above, don't allocate anything extra */
	new_max_len = min_size;
#endif

	if (list->elements == list->initial_elements)
	{
		/*
		 * Replace original in-line allocation with a separate palloc block.
		 * Ensure it is in the same memory context as the List header.  (The
		 * previous List implementation did not offer any guarantees about
		 * keeping all list cells in the same context, but it seems reasonable
		 * to create such a guarantee now.)
		 */
		list->elements = (ListCell *)
			MemoryContextAlloc(GetMemoryChunkContext(list),
							   new_max_len * sizeof(ListCell));
		memcpy(list->elements, list->initial_elements,
			   list->length * sizeof(ListCell));

		/*
		 * We must not move the list header, so it's unsafe to try to reclaim
		 * the initial_elements[] space via repalloc.  In debugging builds,
		 * however, we can clear that space and/or mark it inaccessible.
		 * (wipe_mem includes VALGRIND_MAKE_MEM_NOACCESS.)
		 */
#ifdef CLOBBER_FREED_MEMORY
		wipe_mem(list->initial_elements,
				 list->max_length * sizeof(ListCell));
#else
		VALGRIND_MAKE_MEM_NOACCESS(list->initial_elements,
								   list->max_length * sizeof(ListCell));
#endif
	}
	else
	{
#ifndef DEBUG_LIST_MEMORY_USAGE
		/* Normally, let repalloc deal with enlargement */
		list->elements = (ListCell *) repalloc(list->elements,
											   new_max_len * sizeof(ListCell));
#else
		/*
		 * repalloc() might enlarge the space in-place, which we don't want
		 * for debugging purposes, so forcibly move the data somewhere else.
		 */
		ListCell   *newelements;

		newelements = (ListCell *)
			MemoryContextAlloc(GetMemoryChunkContext(list),
							   new_max_len * sizeof(ListCell));
		memcpy(newelements, list->elements,
			   list->length * sizeof(ListCell));
		pfree(list->elements);
		list->elements = newelements;
#endif
	}

	list->max_length = new_max_len;
}

/*
 * Make room for a new head cell in the given (non-NIL) list.
 *
 * The data in the new head cell is undefined; the caller should be
 * sure to fill it in
 */
static void
new_head_cell(List *list)
{
	/* Enlarge array if necessary */
	if (list->length >= list->max_length)
		enlarge_list(list, list->length + 1);
	/* Now shove the existing data over */
	memmove(&list->elements[1], &list->elements[0],
			list->length * sizeof(ListCell));
	list->length++;
}

/*
 * Make room for a new tail cell in the given (non-NIL) list.
 *
 * The data in the new tail cell is undefined; the caller should be
 * sure to fill it in
 */
static void
new_tail_cell(List *list)
{
	/* Enlarge array if necessary */
	if (list->length >= list->max_length)
		enlarge_list(list, list->length + 1);
	list->length++;
}

/*
 * Append a pointer to the list. A pointer to the modified list is
 * returned. Note that this function may or may not destructively
 * modify the list; callers should always use this function's return
 * value, rather than continuing to use the pointer passed as the
 * first argument.
 */
List *
lappend(List *list, void *datum)
{
	Assert(IsPointerList(list));

	if (list == NIL)
		list = new_list(T_List, 1);
	else
		new_tail_cell(list);

	lfirst(list_tail(list)) = datum;
	check_list_invariants(list);
	return list;
}

/*
 * Prepend a new element to the list. A pointer to the modified list
 * is returned. Note that this function may or may not destructively
 * modify the list; callers should always use this function's return
 * value, rather than continuing to use the pointer passed as the
 * second argument.
 *
 * Caution: before Postgres 8.0, the original List was unmodified and
 * could be considered to retain its separate identity.  This is no longer
 * the case.
 */
List *
lcons(void *datum, List *list)
{
	Assert(IsPointerList(list));

	if (list == NIL)
		list = new_list(T_List, 1);
	else
		new_head_cell(list);

	lfirst(list_head(list)) = datum;
	check_list_invariants(list);
	return list;
}

#endif /* PG_VERSION_NUM */
