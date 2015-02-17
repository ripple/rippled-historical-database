#!/bin/bash

PSQL_CMD="psql"
PSQL_USER=""
PSQL_DATABASE="test_db"

#echo "Initializing $PSQL_DATABASE..."
#$PSQL_CMD -U $PSQL_USER $PSQL_DATABASE -q -f $SCHEMA_FILE

echo "creating database '$PSQL_DATABASE'..."
createdb $PSQL_DATABASE
node_modules/.bin/mocha --ui tdd -R spec test/offline.test.js --dbname $PSQL_DATABASE
echo "removing database '$PSQL_DATABASE'"
dropdb $PSQL_DATABASE
