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

`$ node import/couchdb/backfill --startIndex 2000000 --stopIndex 1000000`

##API Server

Account Transaction History
Retrieve Ripple transaction history for a specific account. All query string parameters are optional.  If none are included, the query returns the last 20 transactions for the specified account.

GET /v1/accounts/{account}/transactions

Parameter | Description | Values 
---  | --- | ---
type | ripple transaction type, accepts comma separated list | `OfferCreate`, `OfferCancel`, `Payment`, `TrustSet`, `AccountSet`, `TicketCreate`
result | ripple transaction result | `tesSUCCESS`, `tecCLAIM`, `tecPATH_PARTIAL`, `tecUNFUNDED_ADD`, `tecUNFUNDED_OFFER`, `tecUNFUNDED_PAYMENT`, `tecFAILED_PROCESSING`, `tecDIR_FULL`, `tecINSUF_RESERVE_LINE`, `tecINSUF_RESERVE_OFFER`, `tecNO_DST`, `tecNO_DST_INSUF_XRP`, `tecNO_LINE_INSUF_RESERVE`, `tecNO_LINE_REDUNDANT`, `tecPATH_DRY`, `tecUNFUNDED`, `tecMASTER_DISABLED`, `tecNO_REGULAR_KEY`, `tecOWNERS`, `tecNO_ISSUER`, `tecNO_AUTH`, `tecNO_LINE`, `tecINSUFF_FEE`, `tecFROZEN`, `tecNO_TARGET`, `tecNO_PERMISSION`, `tecNO_ENTRY`, `tecINSUFFICIENT_RESERVE'
start | start date and time of time range, ISO_8601 format | `2014-11-05T08:00:00-00:00`
end | end date and time of time range, ISO_8601 format | `2014-11-05T14:00:00-00:00`
ledger_min | earliest ledger index to query | integer
ledger_max | latest ledger index to query | integer
limit | number of transactions to return default is 20 | integer
offset | offset returned results | integer
descending | sort order of the query, defaults to true | boolean
binary | return results in binary format, defaults to false | boolean

Example:

https://history-dev.ripple.com:7443/v1/accounts/r3kmLJN5D28dHuH8vZNUZpMC43pEHpaocV/transactions?type=Payment&start=2014-05-02&end=2014-10-02&limit=10&offset=20
