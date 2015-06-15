var Aggregation  = require('../lib/aggregation/payments');
var Parser       = require('../lib/ledgerParser');
var Hbase        = require('../lib/hbase/hbase-client');
var utils        = require('../lib/utils');
var Importer     = require('../lib/ripple-importer');

var options = {
  "logLevel" : 4,
  "hbase" : {
    "prefix" : 'test_',
    "host"   : "54.172.205.78",
    "port"   : 9090
  },
  "ripple" : {
    "trace"                 : false,
    "allow_partial_history" : false,
    "servers" : [
      { "host" : "s-west.ripple.com", "port" : 443, "secure" : true },
      { "host" : "s-east.ripple.com", "port" : 443, "secure" : true }
    ]
  }
};

var EPOCH_OFFSET = 946684800;
var live  = new Importer(options);
var hbase = new Hbase(options.hbase);
var currencies = { };

live.liveStream();
live.on('ledger', function (ledger) {
  processLedger(ledger);
});


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

function processLedger (ledger) {

  var parsed = Parser.parseLedger(ledger);
  parsed.payments.forEach(function(p) {
    var key;

    if (p.currency === 'XRP') {
      key = p.currency;

    } else if (p.issuer) {
      key = p.currency + '|' + p.issuer;

    // should not get here
    } else {
      console.log('missing issuer');
      console.log(p.currency, p.destination_balance_changes);
      process.exit();
      return;
    }

    if (!currencies[key]) {
      currencies[key] = new Aggregation({
        currency: p.currency,
        issuer: p.issuer,
        hbase: hbase,
        logLevel: options.logLevel
      });
    }

    console.log(key);
    currencies[key].add(p, function(err, resp) {
      if (err) console.log(key, err);
      else {
        console.log(key, 'aggregation finished');
      }
    });

  });
}
