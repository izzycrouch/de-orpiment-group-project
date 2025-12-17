#!/bin/bash

set -e

set -a
source .env
set +a

psql -U "$PG_USER" -d postgres <<EOF
DROP DATABASE IF EXISTS $PG_DATABASE;
EOF

psql -U "$PG_USER" -d postgres <<EOF
CREATE DATABASE $PG_DATABASE;
EOF