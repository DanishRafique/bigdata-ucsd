#!/usr/bin/python
import sys

"""
	Reducer for the tv_show mapper results.
	Possible thanks to @ David Tran and his incredibles tips
	on the forum!
"""

prev_word      = None
accumulator	   = 0
print_flag 	   = 0

for line in sys.stdin:
    line       = line.strip()       
    key_value  = line.split(' - ')

    curr_word  = key_value[0]
    value_in   = key_value[1]

    if (prev_word != curr_word):
        if (print_flag):
            print(str(prev_word) + " " + str(accumulator))
            print_flag 	= 0
            accumulator	= 0
        if (value_in != "ABC"):
            accumulator = int(value_in)
    else:
        if (value_in != "ABC"):
            accumulator += int(value_in)
    if (value_in == "ABC"):
    	print_flag = 1

    prev_word = curr_word