
var config = require('../config/import.config');
var Logger = require('../lib/logger');
var importer = require('../lib/ripple-importer');
var hbase = require('./client');

var log = new Logger({
  scope: 'live import',
  level: config.get('logLevel') || 0,
  file: config.get('logFile')
});


//start import stream
importer.liveStream();

log.info('Saving Ledgers to HBase');

importer.on('ledger', function(ledger) {
  hbase.saveLedger(ledger, function(err, resp) {
    if (err) {
      log.error(err);
    }
  });
});


