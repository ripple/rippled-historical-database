Rippled Historical Database
==========================

![Travis Build Status](https://travis-ci.org/ripple/rippled-historical-database.svg?branch=develop)

SQL database as a canonical source of historical data in Ripple

##Setup Instructions

+ install [postgres](http://www.postgresql.org/)
+ install [node.js](http://nodejs.org/) and [npm](https://www.npmjs.org/)
+ `$ git clone https://github.com/ripple/rippled-historical-database.git`
+ `$ cd rippled-historical-database`
+ `$ npm install`
+ create a new postgres database
+ migrate the database to the latest schema
  + `$ node_modules/knex/lib/bin/cli.js migrate:latest`
+ set up config files in `/config`
+ to start the real time importing process into postgres: `$ node import/live`
+ to start the API server: `$ npm start` or `node api/server.js`
  +  `$ npm start` runs nodemon to restart the server whenever the source files change
 
##Importer

The importer connects to rippled via websocket, and listens for ledger closes. upon close, the importer will request the latest validated ledger.  The live import process has some fault tolerance built in to prevent ledgers from being skipped, however it is possible for ledgers to be missed.

There is a secondary process that runs periodically to validate the data already imported and check for gaps in the ledger history.  In addition to that, there is a backfill process that can be triggered manually.

#Live import
Live importing can be done onto one or more data stores concurrently, defaulting to postgres:

`$ node import/live`
`$ node import/live --type hbase`
`$ node import/live --type postgres,couchdb`

#Manual Backfill
Backfilling history can be triggered from the last validated ledger, or a specific ledger range.  The ledger range is inclusive of the start and stop indexes provided.  Start index is defaulted to the last validated ledger index.  Backfilling proceeds backwards in time, so the start index must be greater than the stop index.

`$ node import/postgres/backfill`
`$ node import/couchdb/backfill --startIndex 1000000 --stopIndex 2000000`

##API Server

