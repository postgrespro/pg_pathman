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

static void test_irange_list_union(void **state);


/* Entrypoint */
int
main(void)
{
	/* Array of test functions */
	const struct CMUnitTest tests[] =
	{
		cmocka_unit_test(test_irange_list_union),
	};

	/* Run series of tests */
	return cmocka_run_group_tests(tests, NULL, NULL);
}

/*
 * ----------------------
 *  Definitions of tests
 * ----------------------
 */

static void
test_irange_list_union(void **state)
{
	IndexRange a, b;
	List	  *union_result;


	/* Subtest #0 */
	a = make_irange(0, 100, true);
	b = make_irange(0, 100, true);
	union_result = irange_list_union(list_make1_irange(a),
									 list_make1_irange(b));

	assert_string_equal(rangeset_print(union_result),
						"[0-100]L");

	/* Subtest #1 */
	a = make_irange(0, 100, true);
	b = make_irange(0, 100, false);
	union_result = irange_list_union(list_make1_irange(a),
									 list_make1_irange(b));

	assert_string_equal(rangeset_print(union_result),
						"[0-100]C");

	/* Subtest #2 */
	a = make_irange(0, 100, true);
	b = make_irange(0, 50, false);
	union_result = irange_list_union(list_make1_irange(a),
									 list_make1_irange(b));

	assert_string_equal(rangeset_print(union_result),
						"[0-50]C, [51-100]L");

	/* Subtest #3 */
	a = make_irange(0, 100, true);
	b = make_irange(50, 100, false);
	union_result = irange_list_union(list_make1_irange(a),
									 list_make1_irange(b));

	assert_string_equal(rangeset_print(union_result),
						"[0-49]L, [50-100]C");

	/* Subtest #4 */
	a = make_irange(0, 100, true);
	b = make_irange(50, 99, false);
	union_result = irange_list_union(list_make1_irange(a),
									 list_make1_irange(b));

	assert_string_equal(rangeset_print(union_result),
						"[0-49]L, [50-99]C, 100L");

	/* Subtest #5 */
	a = make_irange(0, 100, true);
	b = make_irange(1, 100, false);
	union_result = irange_list_union(list_make1_irange(a),
									 list_make1_irange(b));

	assert_string_equal(rangeset_print(union_result),
						"0L, [1-100]C");

	/* Subtest #6 */
	a = make_irange(0, 100, true);
	b = make_irange(20, 50, false);
	union_result = irange_list_union(list_make1_irange(a),
									 list_make1_irange(b));

	assert_string_equal(rangeset_print(union_result),
						"[0-19]L, [20-50]C, [51-100]L");
}
