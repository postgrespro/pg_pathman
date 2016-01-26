#include "pathman.h"
#include "storage/shmem.h"
#include "storage/dsm.h"
#include "storage/lwlock.h"
#include <stdint.h>

static Table *table;
static dsm_segment *segment = NULL;


void
alloc_dsm_table()
{
	bool found;
	table = (Table *) ShmemInitStruct("dsm table", sizeof(Table), &found);
	if (!found)
		table->segment_handle = 0;
}


/*
 * Initialize dsm segment. Returns true if new segment was created and
 * false if attached to existing segment
 */
bool
init_dsm_segment(size_t block_size)
{
	bool ret;
	dsm_handle handle;

	/* lock here */
	LWLockAcquire(dsm_init_lock, LW_EXCLUSIVE);

	/* if there is already an existing segment then attach to it */
	if (table->segment_handle != 0)
	{
		ret = false;
		segment = dsm_attach(table->segment_handle);
	}
	
	/*
	 * If segment hasn't been created yet or has already been destroyed
	 * (it happens when last session detaches segment) then create new one
	 */
	if (table->segment_handle == 0 || segment == NULL)
	{
		/* create segment */
		segment = dsm_create(block_size * BLOCKS_COUNT, 0);
		handle = dsm_segment_handle(segment);
		init_dsm_table(table, handle, block_size);
		ret = true;
	}

	/*
	 * Keep mapping till the end of the session. Otherwise it would be
	 * destroyed by the end of transaction
	 */
	dsm_pin_mapping(segment);

	/* unlock here */
	LWLockRelease(dsm_init_lock);

	return ret;
}

void
init_dsm_table(Table *tbl, dsm_handle h, size_t block_size)
{
	int i;
	Block *block;

	memset(table, 0, sizeof(Table));
	table->segment_handle = h;
	table->block_size = block_size;
	table->first_free = 0;

	/* create blocks */
	for (i=0; i<BLOCKS_COUNT; i++)
	{
		block = &table->blocks[i];
		block->segment = h;
		block->offset = i * block_size;
		block->is_free = true;
	}

	return;
}

/*
 * Allocate array inside dsm_segment
 */
void
alloc_dsm_array(DsmArray *arr, size_t entry_size, size_t length)
{
	int		i = 0;
	Block   *block = NULL;
	int		free_count = 0;
	int		size_requested = entry_size * length;
	int min_pos = 0;
	int max_pos = 0;

	for (i = table->first_free; i<BLOCKS_COUNT; i++)
	{
		if (table->blocks[i].is_free)
		{
			if (!block)
			{
				block = &table->blocks[i];
				min_pos = i;
			}
			free_count++;
		}
		else
		{
			free_count = 0;
			block = NULL;
		}

		if (free_count * table->block_size >= size_requested)
		{
			// return block->offset;
			max_pos = i;
			break;
		}
	}

	/* look up for first free block */
	for (i = i+1; i<BLOCKS_COUNT; i++)
		if (table->blocks[i].is_free == true)
		{
			table->first_free = i;
			break;
		}

	/* if we found enough of space */
	if (free_count * table->block_size >= size_requested)
	{
		for(i=min_pos; i<=max_pos; i++)
			table->blocks[i].is_free = false;
		arr->offset = block->offset;
		arr->length = length;
	}
}

void
free_dsm_array(DsmArray *arr)
{
	int start = arr->offset / table->block_size;
	int i = 0;

	/* set blocks free */
	for(;; i++)
	{
		table->blocks[start + i].is_free = true;
		if (i * table->block_size >= arr->length)
			break;
	}

	if (arr->offset < table->first_free)
		table->first_free = arr->offset;

	arr->offset = 0;
	arr->length = 0;
}

void *
dsm_array_get_pointer(const DsmArray* arr)
{
	return (uint8_t *) dsm_segment_address(segment) + arr->offset;
}
