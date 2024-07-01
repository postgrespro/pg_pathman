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
#if PG_VERSION_NUM < 160000
					 const char *errorType,
#endif
					 const char *fileName,
					 int lineNumber)
{
	if (!PointerIsValid(conditionName) || !PointerIsValid(fileName)
#if PG_VERSION_NUM < 160000
		|| !PointerIsValid(errorType)
#endif
		)
	{
		printf("TRAP: ExceptionalCondition: bad arguments\n");
	}
	else
	{
		printf("TRAP: %s(\"%s\", File: \"%s\", Line: %d)\n",
#if PG_VERSION_NUM < 160000
			   errorType,
#else
			   "",
#endif
			   conditionName,
			   fileName, lineNumber);

	}

	/* Usually this shouldn't be needed, but make sure the msg went out */
	fflush(stderr);

	abort();
}
