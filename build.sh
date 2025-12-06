#!/bin/bash

set -xeuo pipefail

clang++-20 -fno-exceptions -Wall -Werror -O3 -g -fno-omit-frame-pointer -march=native -flto=thin -std=c++23 main.cpp -o brc

