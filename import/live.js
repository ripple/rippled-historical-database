var log      = require('../lib/log')('ledgerstream');
var Importer = require('./importer');
var live     = new Importer();
var indexer  = require('./couchdb/indexer');
var couchdb  = require('./couchdb/client');
var config  = require('../config/import.config');
var postgres = new require('./postgres/client.js')(config.get('sql'));
var hbase    = require('./hbase/client');


//start import stream
live.liveStream();

/*
//hbase importer
live.on('ledger', function(ledger) {
  hbase.saveLedger(ledger);
});
*/

//postgres importer
live.on('ledger', function(ledger) {
  postgres.saveLedger(ledger);
});


/*
//couchdb importer
live.on('ledger', function(ledger) {
  couchdb.saveLedger(ledger, function(err, resp){
    if (resp) indexer.pingCouchDB();
  });
});
*/