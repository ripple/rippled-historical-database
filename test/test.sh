#!/bin/bash

PSQL_CMD="psql"
PSQL_USER=""
PSQL_DATABASE="test_db"
TAG="TEST_"$((RANDOM%8999+1000))"_"
#TAG="TEST_"
PORT=7112

#echo "Initializing $PSQL_DATABASE..."
#$PSQL_CMD -U $PSQL_USER $PSQL_DATABASE -q -f $SCHEMA_FILE

echo "creating database '$PSQL_DATABASE'..."
createdb $PSQL_DATABASE
node_modules/.bin/mocha --ui tdd -R spec test/offline.test.js --dbname $PSQL_DATABASE
node_modules/.bin/mocha --ui tdd -R spec test/smoment.test.js
node_modules/.bin/mocha --ui tdd -R spec test/gateways.test.js

if [ "$1" = "hbase" ]; then
  node_modules/.bin/mocha --ui tdd -R spec test/hbaseTests/createTables.js --prefix $TAG --port $PORT
  node_modules/.bin/mocha --ui tdd -R spec test/hbaseTests/importLedgers.js --prefix $TAG --port $PORT
  node_modules/.bin/mocha --ui tdd -R spec test/hbaseTests/setup.js test/hbaseTests/test.*.js --prefix $TAG --port $PORT
  node_modules/.bin/mocha --ui tdd -R spec test/hbaseTests/removeTables.js --prefix $TAG --port $PORT
fi
echo "removing database '$PSQL_DATABASE'"
dropdb $PSQL_DATABASE
