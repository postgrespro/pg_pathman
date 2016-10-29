#include "postgres.h"
#include "nodes/bitmapset.h"


int
bms_next_member(const Bitmapset *a, int prevbit);


int
bms_next_member(const Bitmapset *a, int prevbit)
{
	printf("bms_next_member(): not implemented yet\n");
	fflush(stdout);

	abort();
}
