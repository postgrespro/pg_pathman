#include "pathman.h"
#include "storage/shmem.h"
#include "storage/dsm.h"
#include "storage/lwlock.h"
#include <stdint.h>

// static Table *table;
static dsm_segment *segment = NULL;
static dsm_handle *segment_handle = 0;
static size_t _first_free = 0;
static size_t _block_size = 0;

typedef int BlockHeader;
typedef BlockHeader* BlockHeaderPtr;

#define FREE_BIT 0x80000000
#define is_free(header) \
	((*header) & FREE_BIT)
#define set_free(header) \
	((*header) | FREE_BIT)
#define set_used(header) \
	((*header) & ~FREE_BIT)
#define get_length(header) \
	((*header) & ~FREE_BIT)
#define set_length(header, length) \
	((length) | ((*header) & FREE_BIT))

void
alloc_dsm_table()
{
	bool found;
	segment_handle = ShmemInitStruct("dsm table", sizeof(dsm_handle), &found);
	if (!found)
		*segment_handle = 0;
}


/*
 * Initialize dsm segment. Returns true if new segment was created and
 * false if attached to existing segment
 */
bool
init_dsm_segment(size_t block_size)
{
	bool ret;

	/* lock here */
	LWLockAcquire(dsm_init_lock, LW_EXCLUSIVE);

	/* if there is already an existing segment then attach to it */
	if (*segment_handle != 0)
	{
		ret = false;
		segment = dsm_attach(*segment_handle);
	}
	
	/*
	 * If segment hasn't been created yet or has already been destroyed
	 * (it happens when last session detaches segment) then create new one
	 */
	if (*segment_handle == 0 || segment == NULL)
	{
		/* create segment */
		segment = dsm_create(block_size * BLOCKS_COUNT, 0);
		*segment_handle = dsm_segment_handle(segment);
		init_dsm_table(block_size);
		ret = true;
	}
	_block_size = block_size;

	/*
	 * Keep mapping till the end of the session. Otherwise it would be
	 * destroyed by the end of transaction
	 */
	dsm_pin_mapping(segment);

	/* unlock here */
	LWLockRelease(dsm_init_lock);

	return ret;
}

/*
 * Initialize allocated segment with block structure
 */
void
init_dsm_table(size_t block_size)
{
	int i;
	BlockHeaderPtr header;
	char *ptr = dsm_segment_address(segment);

	/* create blocks */
	for (i=0; i<BLOCKS_COUNT; i++)
	{
		header = (BlockHeaderPtr) &ptr[i * block_size];
		*header = set_free(header);
		*header = set_length(header, 1);
	}
	_first_free = 0;

	return;
}

/*
 * Allocate array inside dsm_segment
 */
void
alloc_dsm_array(DsmArray *arr, size_t entry_size, size_t length)
{
	int		i = 0;
	int		size_requested = entry_size * length;
	int min_pos = 0;
	int max_pos = 0;
	size_t offset = 0;
	size_t total_length = 0;
	char *ptr = dsm_segment_address(segment);
	BlockHeaderPtr header;

	for (i = _first_free; i<BLOCKS_COUNT; )
	{
		header = (BlockHeaderPtr) &ptr[i * _block_size];
		if (is_free(header))
		{
			if (!offset)
			{
				offset = i * _block_size;
				total_length = _block_size - sizeof(BlockHeader);
				min_pos = i;
			}
			else
			{
				total_length += _block_size;
			}
			i++;
		}
		else
		{
			offset = 0;
			total_length = 0;
			i += get_length(header);
		}

		if (total_length >= size_requested)
		{
			max_pos = i-1;
			break;
		}
	}

	/* look up for first free block */
	for (; i<BLOCKS_COUNT; )
	{
		header = (BlockHeaderPtr) &ptr[i * _block_size];
		if (is_free(header))
		{
			_first_free = i;
			break;
		}
		else
		{
			i += get_length(header);
		}
	}

	/* if we found enough of space */
	if (total_length >= size_requested)
	{
		header = (BlockHeaderPtr) &ptr[min_pos * _block_size];
		*header = set_used(header);
		*header = set_length(header, max_pos - min_pos + 1);

		arr->offset = offset;
		arr->length = length;
	}
}

void
free_dsm_array(DsmArray *arr)
{
	int start = arr->offset / _block_size;
	int i = 0;
	char *ptr = dsm_segment_address(segment);
	BlockHeaderPtr header = (BlockHeaderPtr) &ptr[start * _block_size];
	size_t blocks_count = get_length(header);

	/* set blocks free */
	for(; i < blocks_count; i++)
	{
		header = (BlockHeaderPtr) &ptr[(start + i) * _block_size];
		*header = set_free(header);
		*header = set_length(header, 1);
	}

	if (start < _first_free)
		_first_free = start;

	arr->offset = 0;
	arr->length = 0;
}

void *
dsm_array_get_pointer(const DsmArray* arr)
{
	return (char *) dsm_segment_address(segment) + arr->offset + sizeof(BlockHeader);
}
