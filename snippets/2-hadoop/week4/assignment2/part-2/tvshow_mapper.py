#!/usr/bin/python
import sys

"""
	Mapper for the tv_show files.
	Break a line and check if {key: value} respectec
	the following rule: value need to be a integer or
	value need to be "ABC".
"""
for line in sys.stdin:
    line       = line.strip()
    key_value  = line.split(",")
    key_in     = key_value[0]
    value_in   = key_value[1]

    if value_in.isdigit() or value_in == "ABC":
    	print(str(key_in) + " - " + str(value_in))