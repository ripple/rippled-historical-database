var config = require('../config/import.config');
var Hbase  = require('../lib/hbase/hbase-client');
var Parser = require('../lib/ledgerParser');

var hbase = new Hbase(config.get('hbase'));
var hash  = config.get('hash');

hbase.removeLedger(hash, function(err, resp) {
  if (err) {
    console.log('error removing ledger:', err);
  } else {
    console.log('ledger removed', hash);
  }

  process.exit();
});


