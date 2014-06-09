#!/usr/bin/env bash

if ! which tox; then
    echo "Please install tox using `pip install tox`"
    exit 1
fi

tox
