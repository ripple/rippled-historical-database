var Aggregation  = require('../lib/aggregation/exchanges');
var Parser       = require('../lib/ledgerParser');
var Hbase        = require('../lib/hbase/hbase-client');
var utils        = require('../lib/utils');
var Importer     = require('../lib/ripple-importer');

var options = {
  "logLevel" : 4,
  "hbase" : {
    "prefix" : 'stage_',
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
var pairs = [ ];

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
  //hbase.saveParsedData({data:parsed}, function(err, resp) {
    //hbase.saveTransactions(parsed.transactions, function(err, resp) {
      //hbase.saveLedger(parsed.ledger, function(err, resp) {
        console.log('ledger saved:', ledger.ledger_index);

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
              logLevel : options.logLevel
            });
          }

          pairs[pair].add(ex, function(err, resp) {
            if (err) console.log(pair, err);
            else {
              console.log(pair, 'aggregation finished');
            }
          });
         });
      //});
    //});
  //});
}






