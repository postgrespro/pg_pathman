/* ------------------------------------------------------------------------
 *
 * dsm_array.h
 *		Allocate data in shared memory
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#ifndef DSM_ARRAY_H
#define DSM_ARRAY_H

#include "postgres.h"
#include "storage/dsm.h"


/*
 * Dynamic shared memory array
 */
typedef struct
{
	dsm_handle		segment;
	size_t			offset;
	size_t			elem_count;
	size_t			entry_size;
} DsmArray;


#define InvalidDsmArray	{ 0, 0, 0, 0 }


/* Dynamic shared memory functions */
Size estimate_dsm_config_size(void);
void init_dsm_config(void);
bool init_dsm_segment(size_t blocks_count, size_t block_size);
void init_dsm_table(size_t block_size, size_t start, size_t end);
void alloc_dsm_array(DsmArray *arr, size_t entry_size, size_t elem_count);
void free_dsm_array(DsmArray *arr);
void resize_dsm_array(DsmArray *arr, size_t entry_size, size_t elem_count);
void *dsm_array_get_pointer(const DsmArray *arr, bool copy);
dsm_handle get_dsm_array_segment(void);
void attach_dsm_array_segment(void);

#endif
