rippled Historical Database
==========================

![Travis Build Status](https://travis-ci.org/ripple/rippled-historical-database.svg?branch=develop)

The `rippled` Historical Database provides access to raw Ripple transactions that are stored in a database. This provides an alternative to querying `rippled` itself for transactions that occurred in older ledgers, so that the `rippled` server can maintain fewer historical ledger versions and use fewer server resources.

Ripple Labs provides a live instance of the `rippled` Historical Database API with as complete a transaction record as possible at the following address:

`history.ripple.com`

You can also [install and run your own instance of the Historical Database](#running-the-historical-database).

# Usage #

The `rippled` Historical Database provides a REST API, currently with only one API method:

## Get Account Transaction History ##
[[Source]](https://github.com/ripple/rippled-historical-database/blob/8dc88fc5a9de5f6bd12dd3589b586872fe283ad3/api/routes/accountTx.js "Source")

Retrieve a history of transactions that affected a specific account. This includes all transactions the account sent, payments the account received, and payments that rippled through the account.


**REST**

```
GET /v1/accounts/{:address}/transactions
```


The following URL parameters are required by this API endpoint:

| Field | Value | Description |
|-------|-------|-------------|
| account | String | The Ripple account address of the account |

Optionally, you can also include the following query parameters:

| Field | Value | Description |
|-------|-------|-------------|
| type  | Single transaction type or comma-separated list of types | Filter results to include only transactions with the specified transaction type or types. Valid types include `OfferCreate`, `OfferCancel`, `Payment`, `TrustSet`, `AccountSet`, and `TicketCreate`. |
| result | Transaction result code | Filter results to only transactions with the specified transaction result code. Valid result codes include `tesSUCCESS` and all [tec codes](https://ripple.com/build/transactions/#tec-codes). |
| start | ISO 8601-format date (YYYY-MM-DDThh:mmZ) | Only retrieve transactions occurring on or after the specified date and time. |
| end   | ISO 8601-format date (YYYY-MM-DDThh:mmZ) | Only retrieve transactions occurring on or before the specified date and time. |
| ledger_min | Integer | Sequence number of the earliest ledger to search. | 
| ledger_max | Integer | Sequence number of the most recent ledger to search. |
| limit | Integer | Number of transactions to return. Defaults to 20. |
| offset | Integer | Start getting results after skipping this many. Used for paginating results. |
| descending | Boolean | Whether to order transactions with the most recent first. Defaults to true. |
| binary | Boolean | If true, retrieve transactions as hex-formatted binary blobs instead of parsed JSON objects. Defaults to false. |

#### Response Format ####

A successful response uses the HTTP code **200 OK** and has a JSON body with the following:

| Field  | Value | Description |
|--------|-------|-------------|
| result | `success` | Indicates that the body represents a successful response. |
| count  | Integer | The number of objects contained in the `transactions` field. |
| transactions | Array of transaction objects | The transactions matching the request, with a `tx` field for the [transaction definition](https://ripple.com/build/transactions/) and a `meta` field with transaction result metadata. |



# Running the Historical Database #

You can also run your own instance of the Historical Database software, and populate it with transactions from your own `rippled` instance. This is useful if you do not want to depend on Ripple Labs to operate the historical database indefinitely, or you want access to historical transactions from within your own intranet.

## Installation ##

### Dependencies ###

The Historical Database requires the following software installed first:
* [PostgreSQL](http://www.postgresql.org/) (recommended), [HBase](http://hbase.apache.org/), or [CouchDB](http://couchdb.apache.org/).
* [Node.js](http://nodejs.org/)
* [npm](https://www.npmjs.org/)
* [git](http://git-scm.com/) (optional) for installation and updating. 

### Installation Process ###

1. Clone the rippled Historical Database Git Repository:
    `git clone https://github.com/ripple/rippled-historical-database.git`
    (You can also download and extract a zipped release instead.)
2. Use npm to install additional modules:
    `cd rippled-historical-database`
    `npm install`
3. Create a new postgres database:
    `createdb ripple_historical` in PostgreSQL
4. Load the latest database schema for the rippled Historical Database:
    `node_modules/knex/lib/bin/cli.js migrate:latest`
5. Create configuration files (and modify as necessary):
    `cp config/api.config.json.example config/api.config.json`

At this point, the rippled Historical Database is installed. See [Services](#services) for the different components that you can run.

### Services ###

The `rippled` Historical Database consists of several processes that can be run separately. 

* [Live Ledger Importer](#live-ledger-importer) - Monitors `rippled` for newly-validated ledgers.
    `node import/live`
* [Backfiller](#backfiller) - Populates the database with older ledgers from a `rippled` instance.
    `node import/postgres/backfill`
* API Server - Provides [REST API access](#usage) to the data.
    `npm start` (restarts the server automatically when source files change),
    or `node api/server.js` (simple start)

# Importing Data #

In order to retrieve data from the `rippled` Historical Database, you must first populate it with data. Broadly speaking, there are two ways this can happen:

* Connect to a `rippled` server that has the historical ledgers, and retrieve them. (Later, you can reconfigure the `rippled` server not to maintain history older than what you have in your Historical Database.)
    * You can choose to retrieve only new ledgers as they are validated, or you can retrieve old ledgers, too.
* Or, you can load a dump from a database that already has the historical ledger data. (At this time, there are no publicly-available database dumps of historical data.) Use the standard process for your database.

In all cases, keep in mind that the integrity of the data is only as good as the original source. If you retrieve data from a public server, you are assuming that the operator of that server is trustworthy. If you load from a database dump, you are assuming that the provider of the dump has not corrupted or tampered with the data.
 
## Live Ledger Importer ##

The Live Ledger Importer is a service that connects to a `rippled` server using the WebSocket API, and listens for ledger close events. Each time a new ledger is closed, the Importer requests the latest validated ledger. Although this process has some fault tolerance built in to prevent ledgers from being skipped, it is still possible that the Importer may miss ledgers.

The Live Ledger Importer includes a secondary process that runs periodically to validate the data already imported and check for gaps in the ledger history.

The Live Ledger Importer can import to one or more different data stores concurrently. If you have configured the historical database to use another storage scheme, you can use the `--type` parameter to specify the database type or types to use. If not specified, the `rippled` Historical Database defaults to PostgreSQL.

Here are some examples:

```
// defaults to PostgreSQL:
$ node import/live

// Use HBase instead:
$ node import/live --type hbase

// Use PostgreSQL and CouchDB simultaneously:
$ node import/live --type postgres,couchdb
```

## Backfiller ##

The Backfiller retrieves old ledgers from a `rippled` instance by moving backwards in time. You can optionally provide start and stop indexes to retrieve a specific range of ledgers, by their sequence number.

The `--startIndex` parameter defines the most-recent ledger to retrieve. The Backfiller retrieves this ledger first and then continues retrieving progressively older ledgers. If this parameter is omitted, the Backfiller begins with the newest validated ledger.

The `--stopIndex` parameter defines the oldest ledger to retrieve. The Backfiller stops after it retrieves this ledger. If omitted, the Backfiller continues as far back as possible. Because backfilling goes from most recent to least recent, the stop index should be a smaller than the start index.

**Warning:** The Backfiller is best for filling in relatively short histories of transactions. Importing a complete history of all Ripple transactions using the Backfiller could take months. If you want a full history, we recommend acquiring a database dump with early transctions, and importing it directly. Ripple Labs used the internal SQLite database from an offline `rippled` to populate its historical databases with the early transactions, then used the Backfiller to catch up to date after the import finished.

Here are some examples:

```
// retrieve everything to PostgreSQL
node import/postgres/backfill

// get ledgers #1,000,000 to #2,000,000 (inclusive) and store in CouchDB
node import/couchdb/backfill --startIndex 2000000 --stopIndex 1000000
```

