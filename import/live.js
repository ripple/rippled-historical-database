
var config = require('../config/import.config');
var Logger = require('../lib/logger');
var Importer = require('../lib/ripple-importer');
var HBase = require('./client');
var hbase = new HBase();

var live = new Importer({
  ripple: config.get('ripple'),
  logLevel: config.get('logLevel')
});

var log = new Logger({
  scope: 'live import',
  level: config.get('logLevel') || 0,
  file: config.get('logFile')
});


//start import stream
live.liveStream();

log.info('Saving Ledgers to HBase');

live.on('ledger', function(ledger) {
  hbase.saveLedger(ledger, function(err, resp) {
    if (err) {
      log.error(err);
    }
  });
});


