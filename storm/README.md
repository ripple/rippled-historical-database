Storm Ledger Importer
==========================
The ledger importer utilizes Storm to process incoming ledgers and transactions in real time. Transactions are parsed into formats useful for analytics and reporting, and aggregated in various ways suitable for reporting network statistics, monitoring, and visualizing data.

### Dependencies ###

The Storm Ledger Importer requires the following software installed first:
* [Node.js](http://nodejs.org/)
* [npm](https://www.npmjs.org/)
* [git](http://git-scm.com/)
* [Apache Storm](https://storm.apache.org/)
* [Apache Maven](https://maven.apache.org/)
* [HBase](http://hbase.apache.org/) Hbase and thrift server required

### Installation Process ###

* install dependencies
* clone this repository
* from the main directory, run `npm install`
* update `config/import.config.json` to point to your Hbase thrift server
* to run locally:
  * adjust storm parallelism as needed in `storm/local/config.properties`
  * from the command line, run `storm/local/deploy.sh'
* to deploy to a storm cluster:
  * adjust storm parallelism as needed in `storm/production/config.properties`
  * from the command line, run `storm/production/deploy.sh'
