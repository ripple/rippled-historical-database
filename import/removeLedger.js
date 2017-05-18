var config = require('../config')
var hbase = require('../lib/hbase')
var Parser = require('../lib/ledgerParser')
var hash = config.get('hash')

hbase.removeLedger(hash, function(err, resp) {
  if (err) {
    console.log('error removing ledger:', err)
  } else {
    console.log('ledger removed', hash)
  }

  process.exit();
})


