var validator = require('../lib/validator')
var hbase = require('../import/client')
var config = require('../config')

validator.start();
validator.on('ledger', function(ledger, callback) {
  hbase.saveLedger(ledger, function(err, resp) {
    callback(err, resp);
  });
});
