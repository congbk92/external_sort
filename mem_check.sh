#!/bin/sh
valgrind --tool=massif ./main.out testcase/world192.txt testcase/world192_out.txt 8589934592
