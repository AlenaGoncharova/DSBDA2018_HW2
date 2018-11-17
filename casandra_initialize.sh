#!/usr/bin/env bash
cqlsh -f casandra_initialize.cql 127.0.0.1
python3 init_cql.py
cqlsh -f fixtures.cql 127.0.0.1