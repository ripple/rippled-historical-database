var HBase     = require('./client');
var Validator = require('../../lib/validator');
var config    = require('../../config/import.config');
var hbase = new HBase();
var v;

config.logFile = null;
v = new Validator({
  ripple: config.get('ripple'),
  hbase: config.get('hbase'),
  start: config.get('startIndex'),
  recipients: config.get('recipients')
});
v.start();

v.on('ledger', function(ledger, callback) {
  hbase.saveLedger(ledger, function(err, resp) {
    callback(err, resp);
  });
});
