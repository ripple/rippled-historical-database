var config   = require('../config/import.config');
var log      = require('../lib/log')('postgres');
var db = require('../lib/db')(config.get('sql'));
var Import   = require('./importer');
var live     = new Import(config);


live.liveStream();
live.on('ledger', function(ledger) {
	saveLedger(ledger);
});