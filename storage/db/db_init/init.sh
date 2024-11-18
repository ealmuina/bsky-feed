#!/usr/bin/env bash

echo "Creating keyspace"
cqlsh db -f /db_init/schema.cql || true