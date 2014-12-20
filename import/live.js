var config     = require('../config/import.config');
var log        = require('../lib/log')('ledgerstream');
var Importer   = require('./importer');
var aggregator = require('../lib/aggregator');
var live       = new Importer();
var indexer;
var couchdb;
var couchdbValidator;
var postgres;
var postgresValidator;
var hbase;

var typeList = config.get('type') || 'postgres';
var types    = { };
typeList = typeList.split(',');
typeList.forEach(function(type) {
  types[type] = true;
});

//start import stream
live.liveStream();


//hbase importer
if (types.hbase) {
  log.info('Saving Ledgers to HBase');
  hbase = require('./hbase/client');
  
  live.on('ledger', function(ledger) {
    hbase.saveLedger(ledger);
  });
}


//postgres importer
if (types.postgres) {
  log.info('Saving Ledgers to Postgres');
  postgres = new require('./postgres/client');
  postgresValidator = new require('./postgres/validate')();
  
  postgresValidator.start();
  live.on('ledger', function(ledger) {
    postgres.saveLedger(ledger, function(err, resp){
      if (err) {
        log.error('error saving ledger:', err);
      } else {
        log.info('Ledger Saved:', resp.ledger_index);
      }
    });
  });
}

//couchdb importer
if (types.couchdb) {
  log.info('Saving Ledgers to CouchDB');
  indexer = require('./couchdb/indexer');
  couchdb = require('./couchdb/client');
  couchdbValidator = new require('./couchdb/validate')();
  
  couchdbValidator.start();
  live.on('ledger', function(ledger) {
    //aggregator.digestLedger(ledger);
    //return;
    
    couchdb.saveLedger(ledger, function(err, resp){
      if (resp) indexer.pingCouchDB();
      aggregator.digestLedger(ledger);
    });
  });
}