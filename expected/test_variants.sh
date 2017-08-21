#!/usr/bin/bash

ret=0

red="\033[0;31m"
reset='\033[0m'

shopt -s extglob

for result in ./*_+([0-9]).out; do
	f1="$result"
	f2="${f1//_+([0-9])/}"

	printf "examine $(basename $f1) \n"

	file_diff=$(diff $f1 $f2 | wc -l)

	if [ $file_diff -eq 0 ]; then
		printf $red
		printf "WARNING: $(basename $f1) is redundant \n" >&2
		printf $reset

		ret=1 # change exit code
	fi
done

exit $ret
