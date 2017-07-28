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

static void test_irange_basic(void **state);
static void test_irange_change_lossiness(void **state);

static void test_irange_list_union_merge(void **state);
static void test_irange_list_union_lossy_cov(void **state);
static void test_irange_list_union_complete_cov(void **state);
static void test_irange_list_union_intersecting(void **state);

static void test_irange_list_intersection(void **state);


/* Entrypoint */
int
main(void)
{
	/* Array of test functions */
	const struct CMUnitTest tests[] =
	{
		cmocka_unit_test(test_irange_basic),
		cmocka_unit_test(test_irange_change_lossiness),
		cmocka_unit_test(test_irange_list_union_merge),
		cmocka_unit_test(test_irange_list_union_lossy_cov),
		cmocka_unit_test(test_irange_list_union_complete_cov),
		cmocka_unit_test(test_irange_list_union_intersecting),
		cmocka_unit_test(test_irange_list_intersection),
	};

	/* Run series of tests */
	return cmocka_run_group_tests(tests, NULL, NULL);
}

/*
 * ----------------------
 *  Definitions of tests
 * ----------------------
 */

/* Basic behavior tests */
static void
test_irange_basic(void **state)
{
	IndexRange	irange;
	List	   *irange_list;

	/* test irb_pred() */
	assert_int_equal(99, irb_pred(100));
	assert_int_equal(0, irb_pred(1));
	assert_int_equal(0, irb_pred(0));

	/* test irb_succ() */
	assert_int_equal(100, irb_succ(99));
	assert_int_equal(IRANGE_BOUNDARY_MASK, irb_succ(IRANGE_BOUNDARY_MASK));
	assert_int_equal(IRANGE_BOUNDARY_MASK, irb_succ(IRANGE_BOUNDARY_MASK + 1));

	/* test convenience macros */
	irange = make_irange(0, IRANGE_BOUNDARY_MASK, IR_LOSSY);
	assert_int_equal(irange_lower(irange), 0);
	assert_int_equal(irange_upper(irange), IRANGE_BOUNDARY_MASK);
	assert_true(is_irange_lossy(irange));
	assert_true(is_irange_valid(irange));

	/* test allocation */
	irange = make_irange(100, 200, IR_LOSSY);
	irange_list = lappend_irange(NIL, irange);
	assert_memory_equal(&irange, &linitial_irange(irange_list), sizeof(IndexRange));
	assert_memory_equal(&irange, &llast_irange(irange_list), sizeof(IndexRange));

	/* test length */
	irange_list = NIL;
	assert_int_equal(irange_list_length(irange_list), 0);
	irange_list = lappend_irange(irange_list, make_irange(10, 20, IR_LOSSY));
	assert_int_equal(irange_list_length(irange_list), 11);
	irange_list = lappend_irange(irange_list, make_irange(21, 30, IR_LOSSY));
	assert_int_equal(irange_list_length(irange_list), 21);
}


/* Test lossiness switcher */
static void
test_irange_change_lossiness(void **state)
{
	List *irange_list;

	/* test lossiness change (NIL) */
	irange_list = irange_list_set_lossiness(NIL, IR_LOSSY);
	assert_ptr_equal(irange_list, NIL);
	irange_list = irange_list_set_lossiness(NIL, IR_COMPLETE);
	assert_ptr_equal(irange_list, NIL);

	/* test lossiness change (no-op) #1 */
	irange_list = list_make1_irange(make_irange(10, 20, IR_LOSSY));
	irange_list = irange_list_set_lossiness(irange_list, IR_LOSSY);
	assert_string_equal(rangeset_print(irange_list), "[10-20]L");

	/* test lossiness change (no-op) #2 */
	irange_list = list_make1_irange(make_irange(30, 40, IR_COMPLETE));
	irange_list = irange_list_set_lossiness(irange_list, IR_COMPLETE);
	assert_string_equal(rangeset_print(irange_list), "[30-40]C");

	/* test lossiness change (single element) #1 */
	irange_list = list_make1_irange(make_irange(10, 20, IR_LOSSY));
	irange_list = irange_list_set_lossiness(irange_list, IR_COMPLETE);
	assert_string_equal(rangeset_print(irange_list), "[10-20]C");

	/* test lossiness change (single element) #2 */
	irange_list = list_make1_irange(make_irange(30, 40, IR_COMPLETE));
	irange_list = irange_list_set_lossiness(irange_list, IR_LOSSY);
	assert_string_equal(rangeset_print(irange_list), "[30-40]L");

	/* test lossiness change (multiple elements, adjacent) #1 */
	irange_list = list_make1_irange(make_irange(10, 20, IR_LOSSY));
	irange_list = lappend_irange(irange_list, make_irange(21, 40, IR_COMPLETE));
	irange_list = irange_list_set_lossiness(irange_list, IR_COMPLETE);
	assert_string_equal(rangeset_print(irange_list), "[10-40]C");

	/* test lossiness change (multiple elements, adjacent) #2 */
	irange_list = list_make1_irange(make_irange(10, 20, IR_COMPLETE));
	irange_list = lappend_irange(irange_list, make_irange(21, 40, IR_LOSSY));
	irange_list = irange_list_set_lossiness(irange_list, IR_LOSSY);
	assert_string_equal(rangeset_print(irange_list), "[10-40]L");

	/* test lossiness change (multiple elements, non-adjacent) #1 */
	irange_list = list_make1_irange(make_irange(10, 15, IR_COMPLETE));
	irange_list = lappend_irange(irange_list, make_irange(21, 40, IR_LOSSY));
	irange_list = irange_list_set_lossiness(irange_list, IR_COMPLETE);
	assert_string_equal(rangeset_print(irange_list), "[10-15]C, [21-40]C");

	/* test lossiness change (multiple elements, non-adjacent) #2 */
	irange_list = list_make1_irange(make_irange(10, 15, IR_LOSSY));
	irange_list = lappend_irange(irange_list, make_irange(21, 40, IR_COMPLETE));
	irange_list = irange_list_set_lossiness(irange_list, IR_LOSSY);
	assert_string_equal(rangeset_print(irange_list), "[10-15]L, [21-40]L");
}


/* Test merges of adjoint IndexRanges */
static void
test_irange_list_union_merge(void **state)
{
	IndexRange	a, b;
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

	union_result = irange_list_union(unmerged, unmerged);

	assert_string_equal(rangeset_print(union_result),
						"[9-10]C, 11L, [12-24]C");


	/* Subtest #1 */
	a = make_irange(0, 10, IR_COMPLETE);
	b = make_irange(12, 20, IR_COMPLETE);
	union_result = irange_list_union(list_make1_irange(a),
									 list_make1_irange(b));

	assert_string_equal(rangeset_print(union_result),
						"[0-10]C, [12-20]C");

	/* Subtest #2 */
	a = make_irange(0, 10, IR_LOSSY);
	b = make_irange(11, 20, IR_LOSSY);
	union_result = irange_list_union(list_make1_irange(a),
									 list_make1_irange(b));

	assert_string_equal(rangeset_print(union_result),
						"[0-20]L");

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

/* Several IndexRanges intersect, unite them */
static void
test_irange_list_union_intersecting(void **state)
{
	IndexRange	a, b;
	List	   *unmerged,
			   *union_result;


	/* Subtest #0 */
	a = make_irange(0, 55, IR_COMPLETE);
	b = make_irange(55, 100, IR_COMPLETE);
	union_result = irange_list_union(list_make1_irange(a),
									 list_make1_irange(b));

	assert_string_equal(rangeset_print(union_result),
						"[0-100]C");

	/* Subtest #1 */
	a = make_irange(0, 55, IR_COMPLETE);
	b = make_irange(55, 100, IR_LOSSY);
	union_result = irange_list_union(list_make1_irange(a),
									 list_make1_irange(b));

	assert_string_equal(rangeset_print(union_result),
						"[0-55]C, [56-100]L");

	/* Subtest #2 */
	unmerged = NIL;
	unmerged = lappend_irange(unmerged, make_irange(0, 45, IR_LOSSY));
	unmerged = lappend_irange(unmerged, make_irange(100, 100, IR_LOSSY));
	b = make_irange(40, 65, IR_COMPLETE);
	union_result = irange_list_union(unmerged, list_make1_irange(b));

	assert_string_equal(rangeset_print(union_result),
						"[0-39]L, [40-65]C, 100L");

	/* Subtest #3 */
	unmerged = NIL;
	unmerged = lappend_irange(unmerged, make_irange(0, 45, IR_LOSSY));
	unmerged = lappend_irange(unmerged, make_irange(64, 100, IR_LOSSY));
	b = make_irange(40, 65, IR_COMPLETE);
	union_result = irange_list_union(unmerged, list_make1_irange(b));

	assert_string_equal(rangeset_print(union_result),
						"[0-39]L, [40-65]C, [66-100]L");

	/* Subtest #4 */
	unmerged = NIL;
	unmerged = lappend_irange(unmerged, make_irange(0, 45, IR_COMPLETE));
	unmerged = lappend_irange(unmerged, make_irange(64, 100, IR_COMPLETE));
	b = make_irange(40, 65, IR_COMPLETE);
	union_result = irange_list_union(unmerged, list_make1_irange(b));

	assert_string_equal(rangeset_print(union_result),
						"[0-100]C");

	/* Subtest #5 */
	unmerged = NIL;
	unmerged = lappend_irange(unmerged, make_irange(0, 45, IR_COMPLETE));
	unmerged = lappend_irange(unmerged, make_irange(64, 100, IR_COMPLETE));
	b = make_irange(40, 65, IR_LOSSY);
	union_result = irange_list_union(unmerged, list_make1_irange(b));

	assert_string_equal(rangeset_print(union_result),
						"[0-45]C, [46-63]L, [64-100]C");
}


/* Test intersection of IndexRanges */
static void
test_irange_list_intersection(void **state)
{
	IndexRange	a, b;
	List	   *intersection_result,
			   *left_list,
			   *right_list;


	/* Subtest #0 */
	a = make_irange(0, 100, IR_LOSSY);
	b = make_irange(10, 20, IR_LOSSY);

	intersection_result = irange_list_intersection(list_make1_irange(a),
												   list_make1_irange(b));

	assert_string_equal(rangeset_print(intersection_result),
						"[10-20]L");

	/* Subtest #1 */
	a = make_irange(0, 100, IR_LOSSY);
	b = make_irange(10, 20, IR_COMPLETE);

	intersection_result = irange_list_intersection(list_make1_irange(a),
												   list_make1_irange(b));

	assert_string_equal(rangeset_print(intersection_result),
						"[10-20]L");

	/* Subtest #2 */
	a = make_irange(0, 100, IR_COMPLETE);
	b = make_irange(10, 20, IR_LOSSY);

	intersection_result = irange_list_intersection(list_make1_irange(a),
												   list_make1_irange(b));

	assert_string_equal(rangeset_print(intersection_result),
						"[10-20]L");

	/* Subtest #3 */
	a = make_irange(15, 25, IR_COMPLETE);
	b = make_irange(10, 20, IR_LOSSY);

	intersection_result = irange_list_intersection(list_make1_irange(a),
												   list_make1_irange(b));

	assert_string_equal(rangeset_print(intersection_result),
						"[15-20]L");

	/* Subtest #4 */
	a = make_irange(15, 25, IR_COMPLETE);
	b = make_irange(10, 20, IR_COMPLETE);

	intersection_result = irange_list_intersection(list_make1_irange(a),
												   list_make1_irange(b));

	assert_string_equal(rangeset_print(intersection_result),
						"[15-20]C");

	/* Subtest #5 */
	left_list = NIL;
	left_list = lappend_irange(left_list, make_irange(0, 11, IR_LOSSY));
	left_list = lappend_irange(left_list, make_irange(12, 20, IR_COMPLETE));
	right_list = NIL;
	right_list = lappend_irange(right_list, make_irange(1, 15, IR_COMPLETE));
	right_list = lappend_irange(right_list, make_irange(16, 20, IR_LOSSY));

	intersection_result = irange_list_intersection(left_list, right_list);

	assert_string_equal(rangeset_print(intersection_result),
						"[1-11]L, [12-15]C, [16-20]L");

	/* Subtest #6 */
	left_list = NIL;
	left_list = lappend_irange(left_list, make_irange(0, 11, IR_LOSSY));
	left_list = lappend_irange(left_list, make_irange(12, 20, IR_COMPLETE));
	right_list = NIL;
	right_list = lappend_irange(right_list, make_irange(1, 15, IR_COMPLETE));
	right_list = lappend_irange(right_list, make_irange(16, 20, IR_COMPLETE));

	intersection_result = irange_list_intersection(left_list, right_list);

	assert_string_equal(rangeset_print(intersection_result),
						"[1-11]L, [12-20]C");

	/* Subtest #7 */
	a = make_irange(0, 10, IR_COMPLETE);
	b = make_irange(20, 20, IR_COMPLETE);

	intersection_result = irange_list_intersection(list_make1_irange(a),
												   list_make1_irange(b));

	assert_string_equal(rangeset_print(intersection_result),
						""); /* empty set */

	/* Subtest #8 */
	a = make_irange(0, 10, IR_LOSSY);
	right_list = NIL;
	right_list = lappend_irange(right_list, make_irange(10, 10, IR_COMPLETE));
	right_list = lappend_irange(right_list, make_irange(16, 20, IR_LOSSY));

	intersection_result = irange_list_intersection(list_make1_irange(a),
												   right_list);

	assert_string_equal(rangeset_print(intersection_result),
						"10L");

	/* Subtest #9 */
	left_list = NIL;
	left_list = lappend_irange(left_list, make_irange(15, 15, IR_LOSSY));
	left_list = lappend_irange(left_list, make_irange(25, 25, IR_COMPLETE));
	right_list = NIL;
	right_list = lappend_irange(right_list, make_irange(0, 20, IR_COMPLETE));
	right_list = lappend_irange(right_list, make_irange(21, 40, IR_LOSSY));

	intersection_result = irange_list_intersection(left_list, right_list);

	assert_string_equal(rangeset_print(intersection_result),
						"15L, 25L");

	/* Subtest #10 */
	left_list = NIL;
	left_list = lappend_irange(left_list, make_irange(21, 21, IR_LOSSY));
	left_list = lappend_irange(left_list, make_irange(22, 22, IR_COMPLETE));
	right_list = NIL;
	right_list = lappend_irange(right_list, make_irange(0, 21, IR_COMPLETE));
	right_list = lappend_irange(right_list, make_irange(22, 40, IR_LOSSY));

	intersection_result = irange_list_intersection(left_list, right_list);

	assert_string_equal(rangeset_print(intersection_result),
						"[21-22]L");

	/* Subtest #11 */
	left_list = NIL;
	left_list = lappend_irange(left_list, make_irange(21, 21, IR_LOSSY));
	left_list = lappend_irange(left_list, make_irange(22, 25, IR_COMPLETE));
	right_list = NIL;
	right_list = lappend_irange(right_list, make_irange(0, 21, IR_COMPLETE));
	right_list = lappend_irange(right_list, make_irange(22, 40, IR_COMPLETE));

	intersection_result = irange_list_intersection(left_list, right_list);

	assert_string_equal(rangeset_print(intersection_result),
						"21L, [22-25]C");
}
