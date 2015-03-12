var Aggregation  = require('../src/lib/statsAggregation');
var Parser       = require('../src/lib/modules/ledgerParser');
var Hbase        = require('../src/lib/hbase-client');
var utils        = require('../src/lib/utils');
var fs           = require('fs');
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

var stats        = new Aggregation(options);
var path         = __dirname + '/ledgers/';
var EPOCH_OFFSET = 946684800;
var files        = fs.readdirSync(path);
var ledgers      = [ ];

console.log(stats.purge);

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

  var ledger = JSON.parse(fs.readFileSync(path + filename, "utf8"));

  //adjust the close time to unix epoch
  ledger.close_time = ledger.close_time + EPOCH_OFFSET;

  ledger.transactions.forEach(function(tx) {
    var parsed;

    tx     = prepareTransaction(ledger, tx);
    parsed = Parser.parseTransaction(tx);

    //increment transactions
    stats.update({
      label : 'transaction_count',
      data  : {
        count  : 1,
        time   : tx.executed_time
      }
    });

    //aggregate by transaction type
    stats.update({
      label : 'transaction_type',
      data  : {
        type   : tx.TransactionType,
        time   : tx.executed_time
      }
    });

    //aggregate by transaction result
    stats.update({
      label : 'transaction_result',
      data  : {
        result : tx.tx_result,
        time   : tx.executed_time
      }
    });

    //new account created
    if (parsed.accountsCreated.length) {
      stats.update({
        label : 'accounts_created',
        data  : {
          count  : 1,
          time   : tx.executed_time
        }
      });
    }
  });
});

