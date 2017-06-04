var Aggregation  = require('../../lib/aggregation/exchanges');
var Parser       = require('../../lib/ledgerParser');
var Hbase        = require('../../lib/hbase');
var utils        = require('../../lib/utils');
var fs           = require('fs');
var moment       = require('moment');
var config       = require('../../config/test.config');
var options = {
  logLevel : 4,
  hbase: config.get('hbase')
};

options.hbase.prefix = 'test_';
options.hbase.logLevel = options.logLevel;

var path         = __dirname + '/../ledgers/';
var EPOCH_OFFSET = 946684800;
var files        = fs.readdirSync(path);

var ledgers   = [ ];
var exchanges = [ ];
var pairs     = { };
var count     = 0;
var hbase     = new Hbase(options.hbase);

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


files.forEach(function(filename) {
  if (exchanges.length) return;

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
    parsed.exchanges.forEach(function(ex) {
      var pair = ex.base.currency +
        (ex.base.issuer ? "." + ex.base.issuer : '') +
        '/' + ex.counter.currency +
        (ex.counter.issuer ? "." + ex.counter.issuer : '');

      if (!pairs[pair]) {
        pairs[pair] = new Aggregation({
          base     : ex.base,
          counter  : ex.counter,
          hbase    : hbase,
          logLevel : options.logLevel,
          earliest : moment.unix(ex.time).utc()
        });
      }

      console.log(pair);
      pairs[pair].add(ex, function(err, resp) {
        if (err) console.log(err);
      });
    });
  });
}
