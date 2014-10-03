var log      = require('../lib/log')('ledgerstream');
var Importer = require('./importer');
var live     = new Importer();
var indexer  = require('./couchdb/indexer');
var couchdb  = require('./couchdb/client');
var config  = require('../config/import.config');
var postgres = new require('./postgres/client.js')(config.get('sql'));

//start import stream
live.liveStream();

// Run database migrations
postgres.migrate().then(function() {

  //postgres importer
  live.on('ledger', function(ledger) {
    postgres.saveLedger(ledger);
  });
}).done();

/*
//couchdb importer
live.on('ledger', function(ledger) {
  couchdb.saveLedger(ledger, function(err, resp){
    if (resp) indexer.pingCouchDB();
  });
});
*/