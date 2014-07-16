#!/bin/bash

#############################################################################
#
# init_db.sh
#
# Bash shell script to initialise the Ripple History database using a
# predefined schema file.
#
# Modify the following constants to configure the way this script works:

PSQL_CMD="psql"
PSQL_USER="stevenzeiler"
PSQL_DATABASE="test_db"
SCHEMA_FILE="schema.sql"

#############################################################################

echo "Initializing $PSQL_DATABASE..."
$PSQL_CMD -U $PSQL_USER $PSQL_DATABASE -q -f $SCHEMA_FILE

