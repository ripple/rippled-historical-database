var Aggregation  = require('../src/lib/exchangeAggregation');
var Importer     = require('../src/lib/modules/ripple-importer');
var Parser       = require('../src/lib/modules/ledgerParser');
var Hbase        = require('../src/lib/hbase-client');
var utils        = require('../src/lib/utils');
var fs           = require('fs');
/*
var rest = require('../src/lib/modules/hbase-rest')({
  "host" : "54.164.78.183",
  "port" : 20550,
  "prefix" : 'beta2_'
});

rest.initTables();
return;
*/
var EPOCH_OFFSET = 946684800;
var options = {
  "logLevel" : 3,
  "hbase" : {
    "prefix" : 'beta2_',
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

var live  = new Importer(options);
var pairs = { };
var hbase = new Hbase(options.hbase);
hbase.connect();

live.liveStream();
live.on('ledger', function (ledger) {  
  
  //adjust the close time to unix epoch
  ledger.close_time = ledger.close_time + EPOCH_OFFSET;
  
  ledger.transactions.forEach(function(tx) {
    var parsed;
    
    tx        = prepareTransaction(ledger, tx);
    parsed    = Parser.parseTransaction(tx);
    parsed.exchanges.forEach(function(ex) {
      var pair = ex.base.currency + 
        (ex.base.issuer ? "." + ex.base.issuer : '') +
        '/' + ex.counter.currency + 
        (ex.counter.issuer ? "." + ex.counter.issuer : '');
      
      console.log(pair);
      if (!pairs[pair]) {
        pairs[pair] = new Aggregation({
          base     : ex.base,
          counter  : ex.counter,
          hbase    : hbase,
          logLevel : options.logLevel
        });
      }

      pairs[pair].add(ex, function(err, resp) {
        console.log('complete');
      });
      
    });
  });
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
