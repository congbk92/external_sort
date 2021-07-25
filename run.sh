#!/bin/sh
BASEDIR=$(dirname "$0")
${BASEDIR}/main.out ${BASEDIR}/testcase/world192.txt ${BASEDIR}/testcase/world192_out.txt 16384
