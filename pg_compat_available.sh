#!/usr/bin/bash

dir=$(dirname $0)
func="$1"

grep -n -r --include=pg_compat.c --include=pg_compat.h $func $dir | head -n1
