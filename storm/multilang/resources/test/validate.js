var Validator = require('../src/lib/validator');
var config    = require('../config');
var v;

config.logFile = null;
v = new Validator(config);
v.start();

v.on('ledger', function(ledger, callback) {
  console.log(ledger, callback);
});