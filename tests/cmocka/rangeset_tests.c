#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

#include "rangeset.h"

/* for "print" functions */
#include "debug_print.c"


/*
 * -----------------------
 *  Declarations of tests
 * -----------------------
 */

static void test_irange_list_union_merge(void **state);
static void test_irange_list_union_lossy_cov(void **state);
static void test_irange_list_union_complete_cov(void **state);


/* Entrypoint */
int
main(void)
{
	/* Array of test functions */
	const struct CMUnitTest tests[] =
	{
		cmocka_unit_test(test_irange_list_union_merge),
		cmocka_unit_test(test_irange_list_union_lossy_cov),
		cmocka_unit_test(test_irange_list_union_complete_cov),
	};

	/* Run series of tests */
	return cmocka_run_group_tests(tests, NULL, NULL);
}

/*
 * ----------------------
 *  Definitions of tests
 * ----------------------
 */

/* Test merges of adjoint lists */
static void
test_irange_list_union_merge(void **state)
{
	IndexRange	a;
	List	   *unmerged,
			   *union_result;


	/* Subtest #0 */
	a = make_irange(0, 8, IR_COMPLETE);

	unmerged = NIL;
	unmerged = lappend_irange(unmerged, make_irange(9, 10, IR_COMPLETE));
	unmerged = lappend_irange(unmerged, make_irange(11, 11, IR_LOSSY));
	unmerged = lappend_irange(unmerged, make_irange(12, 12, IR_COMPLETE));
	unmerged = lappend_irange(unmerged, make_irange(13, 13, IR_COMPLETE));
	unmerged = lappend_irange(unmerged, make_irange(14, 24, IR_COMPLETE));
	unmerged = lappend_irange(unmerged, make_irange(15, 20, IR_COMPLETE));

	union_result = irange_list_union(list_make1_irange(a), unmerged);

	assert_string_equal(rangeset_print(union_result),
						"[0-10]C, 11L, [12-24]C");
}

/* Lossy IndexRange covers complete IndexRange */
static void
test_irange_list_union_lossy_cov(void **state)
{
	IndexRange	a, b;
	List	   *union_result;


	/* Subtest #0 */
	a = make_irange(0, 100, IR_LOSSY);
	b = make_irange(0, 100, IR_LOSSY);
	union_result = irange_list_union(list_make1_irange(a),
									 list_make1_irange(b));

	assert_string_equal(rangeset_print(union_result),
						"[0-100]L");

	/* Subtest #1 */
	a = make_irange(0, 100, IR_LOSSY);
	b = make_irange(0, 100, IR_COMPLETE);
	union_result = irange_list_union(list_make1_irange(a),
									 list_make1_irange(b));

	assert_string_equal(rangeset_print(union_result),
						"[0-100]C");

	/* Subtest #2 */
	a = make_irange(0, 100, IR_LOSSY);
	b = make_irange(0, 50, IR_COMPLETE);
	union_result = irange_list_union(list_make1_irange(a),
									 list_make1_irange(b));

	assert_string_equal(rangeset_print(union_result),
						"[0-50]C, [51-100]L");

	/* Subtest #3 */
	a = make_irange(0, 100, IR_LOSSY);
	b = make_irange(50, 100, IR_COMPLETE);
	union_result = irange_list_union(list_make1_irange(a),
									 list_make1_irange(b));

	assert_string_equal(rangeset_print(union_result),
						"[0-49]L, [50-100]C");

	/* Subtest #4 */
	a = make_irange(0, 100, IR_LOSSY);
	b = make_irange(50, 99, IR_COMPLETE);
	union_result = irange_list_union(list_make1_irange(a),
									 list_make1_irange(b));

	assert_string_equal(rangeset_print(union_result),
						"[0-49]L, [50-99]C, 100L");

	/* Subtest #5 */
	a = make_irange(0, 100, IR_LOSSY);
	b = make_irange(1, 100, IR_COMPLETE);
	union_result = irange_list_union(list_make1_irange(a),
									 list_make1_irange(b));

	assert_string_equal(rangeset_print(union_result),
						"0L, [1-100]C");

	/* Subtest #6 */
	a = make_irange(0, 100, IR_LOSSY);
	b = make_irange(20, 50, IR_COMPLETE);
	union_result = irange_list_union(list_make1_irange(a),
									 list_make1_irange(b));

	assert_string_equal(rangeset_print(union_result),
						"[0-19]L, [20-50]C, [51-100]L");
}

/* Complete IndexRange covers lossy IndexRange */
static void
test_irange_list_union_complete_cov(void **state)
{
	IndexRange	a, b;
	List	   *union_result;


	/* Subtest #0 */
	a = make_irange(0, 100, IR_COMPLETE);
	b = make_irange(0, 100, IR_LOSSY);
	union_result = irange_list_union(list_make1_irange(a),
									 list_make1_irange(b));

	assert_string_equal(rangeset_print(union_result),
						"[0-100]C");

	/* Subtest #1 */
	a = make_irange(0, 100, IR_COMPLETE);
	b = make_irange(20, 50, IR_LOSSY);
	union_result = irange_list_union(list_make1_irange(a),
									 list_make1_irange(b));

	assert_string_equal(rangeset_print(union_result),
						"[0-100]C");

	/* Subtest #2 */
	a = make_irange(0, 100, IR_COMPLETE);
	b = make_irange(0, 50, IR_LOSSY);
	union_result = irange_list_union(list_make1_irange(a),
									 list_make1_irange(b));

	assert_string_equal(rangeset_print(union_result),
						"[0-100]C");

	/* Subtest #3 */
	a = make_irange(0, 100, IR_COMPLETE);
	b = make_irange(50, 100, IR_LOSSY);
	union_result = irange_list_union(list_make1_irange(a),
									 list_make1_irange(b));

	assert_string_equal(rangeset_print(union_result),
						"[0-100]C");
}
