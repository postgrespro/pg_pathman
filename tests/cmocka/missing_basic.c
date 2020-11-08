#include <stdio.h>

#include "postgres.h"
#include "undef_printf.h"


void *
palloc(Size size)
{
	return malloc(size);
}

void *
repalloc(void *pointer, Size size)
{
	return realloc(pointer, size);
}

void
pfree(void *pointer)
{
	free(pointer);
}

void
ExceptionalCondition(const char *conditionName,
					 const char *errorType,
					 const char *fileName,
					 int lineNumber)
{
	if (!PointerIsValid(conditionName) ||
		!PointerIsValid(fileName) ||
		!PointerIsValid(errorType))
	{
		printf("TRAP: ExceptionalCondition: bad arguments\n");
	}
	else
	{
		printf("TRAP: %s(\"%s\", File: \"%s\", Line: %d)\n",
			   errorType, conditionName,
			   fileName, lineNumber);

	}

	/* Usually this shouldn't be needed, but make sure the msg went out */
	fflush(stderr);

	abort();
}
