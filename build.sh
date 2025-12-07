#!/bin/bash

set -xeuo pipefail

clang++-20 \
-mllvm -inline-threshold=1000 \
-static \
-fno-exceptions \
-Wall \
-Werror \
-O3 \
-g \
-fno-omit-frame-pointer \
-march=native \
-flto \
-std=c++23 \
main.cpp \
-o brc

