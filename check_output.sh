#!/bin/sh
'diff <(cat test*.dat | sort) /global/cscratch1/sd/gguidi/autograder/data/large_solution.txt' > t.lis
if [[ -s t.lis ]] ; then
	echo '0' > is_correct.txt # if return garbage
else
	echo '1' > is_correct.txt # if does not return anything (correct)
fi
