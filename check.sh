#!/bin/bash

RES="/tmp/my_brc_result.txt"

./build.sh && ./brc > ${RES}

r=$(diff ${RES} answer.txt | wc -l)
if [[ r -eq 0 ]];
then
    echo OK
    exit 0
else
    echo FAIL
    exit 1
fi

