#!/bin/bash
set -e

TAG="TEST_"$((RANDOM%8999+1000))"_"
#TAG="TEST_"
PORT=7112
LOG=0

node_modules/.bin/mocha --ui tdd -R spec test/smoment.test.js --logLevel $LOG
node_modules/.bin/mocha --ui tdd -R spec test/gateways.test.js --logLevel $LOG

node_modules/.bin/mocha --ui tdd -R spec test/hbaseTests/createTables.js --prefix $TAG --port $PORT
node_modules/.bin/mocha --ui tdd -R spec test/hbaseTests/importLedgers.js --logLevel $LOG --prefix $TAG --port $PORT
node_modules/.bin/mocha --ui tdd -R spec test/hbaseTests/setup.js test/hbaseTests/test.*.js --logLevel $LOG --prefix $TAG --port $PORT
node_modules/.bin/mocha --ui tdd -R spec test/hbaseTests/removeTables.js --prefix $TAG --port $PORT
