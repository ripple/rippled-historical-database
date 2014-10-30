var config   = require('../config/import.config');
var log      = require('../lib/log')('ledgerstream');
var Importer = require('./importer');
var live     = new Importer();
var indexer  = require('./couchdb/indexer');
var couchdb  = require('./couchdb/client');
var postgres = new require('./postgres/client');
//var hbase    = require('./hbase/client');

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
  live.on('ledger', function(ledger) {
    hbase.saveLedger(ledger);
  });
}


//postgres importer
if (types.postgres) {
  log.info('Saving Ledgers to Postgres');
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
  live.on('ledger', function(ledger) {
    couchdb.saveLedger(ledger, function(err, resp){
      if (resp) indexer.pingCouchDB();
    });
  });
}