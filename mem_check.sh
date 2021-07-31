#!/bin/sh
#valgrind --tool=massif ./main.out testcase/world192_100.txt testcase/world192_100_out.txt 8388608
valgrind ./main.out testcase/world192.txt testcase/world192_out.txt 83886