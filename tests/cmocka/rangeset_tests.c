#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

#include "rangeset.h"

/* for "print" functions */
#include "debug_print.c"


/* declarations of tests */
static void test_1(void **state);


/* Entrypoint */
int
main(void)
{
	const struct CMUnitTest tests[] =
	{
		cmocka_unit_test(test_1),
	};

	return cmocka_run_group_tests(tests, NULL, NULL);
}

/*
 * ----------------------
 *  Definitions of tests
 * ----------------------
 */

static void
test_1(void **state)
{
	IndexRange a = make_irange(0, 100, true),
			   b = make_irange(20, 50, false);

	List	  *union_result;

	union_result = irange_list_union(list_make1_irange(a),
									 list_make1_irange(b));

	assert_string_equal(rangeset_print(union_result),
						"[0-19]L, [20-50]C, [51-100]L");
}

