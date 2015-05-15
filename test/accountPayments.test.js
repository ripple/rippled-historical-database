var Aggregation  = require('../lib/aggregation/accountPayments');
var Parser       = require('../lib/ledgerParser');
var Hbase        = require('../lib/hbase/hbase-client');
var utils        = require('../lib/utils');
var fs           = require('fs');
var config       = require('../config/import.config');
var options = config.get('hbase')


options.prefix = 'test_';
options.logLevel = 4;

var payments     = new Aggregation(options);
var path         = __dirname + '/ledgers/';
var EPOCH_OFFSET = 946684800;
var files        = fs.readdirSync(path);
var ledgers      = [ ];

function prepareTransaction (ledger, tx) {
  var meta = tx.metaData;
  delete tx.metaData;

  tx.raw           = utils.toHex(tx);
  tx.meta          = utils.toHex(meta);
  tx.metaData      = meta;

  tx.ledger_hash   = ledger.ledger_hash;
  tx.ledger_index  = ledger.ledger_index;
  tx.executed_time = ledger.close_time;
  tx.tx_index      = tx.metaData.TransactionIndex;
  tx.tx_result     = tx.metaData.TransactionResult;

  return tx;
};

console.log('# ledgers:', files.length);

var count = 0;

files.forEach(function(filename) {
  var ledger = JSON.parse(fs.readFileSync(path + filename, "utf8"));

  //adjust the close time to unix epoch
  ledger.close_time = ledger.close_time + EPOCH_OFFSET;

  setTimeout(function() {
    console.log("processing ledger:", ledger.ledger_index);
    processLedger(ledger);
  }, count++ * 250);
});

function processLedger (ledger) {

  ledger.transactions.forEach(function(tx) {
    var parsed;

    tx     = prepareTransaction(ledger, tx);
    parsed = Parser.parseTransaction(tx);

    if (parsed.payments.length) {
      payments.add({
        data    : parsed.payments[0],
        account : parsed.payments[0].source
      });

      payments.add({
        data    : parsed.payments[0],
        account : parsed.payments[0].destination
      });
    }
  });
}
