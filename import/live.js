var log      = require('../lib/log')('ledgerstream');
var Importer = require('./importer');
var live     = new Importer();
var indexer  = require('./couchdb/indexer');
var couchdb  = require('./couchdb/client');
var config  = require('../config/import.config');
var postgres = new require('./postgres/db.js')(config.get('sql'));

live.liveStream();
live.on('ledger', function(ledger) {
	postgres.saveLedger(ledger);
/*
  couchdb.saveLedger(ledger, function(err, resp){
    if (resp) indexer.pingCouchDB();
  });
*/
});