var log      = require('../lib/log')('ledgerstream');
var Importer = require('./importer');
var live     = new Importer();
var indexer  = require('./couchdb/indexer');
var couchdb  = require('./couchdb/client');

live.liveStream();
live.on('ledger', function(ledger) {
  
  couchdb.saveLedger(ledger, function(err, resp){
    if (resp) indexer.pingCouchDB();
  });
});