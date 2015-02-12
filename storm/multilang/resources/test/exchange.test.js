var Aggregation  = require('../src/lib/exchangeAggregation');
var Parser       = require('../src/lib/modules/ledgerParser');
var Hbase        = require('../src/lib/hbase-client');
var utils        = require('../src/lib/utils');
var fs           = require('fs');
var options = {
  "logLevel" : 3,
  "hbase" : {
    "prefix" : 'TEST_',
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

var path         = __dirname + '/ledgers/';
var EPOCH_OFFSET = 946684800;
var files        = fs.readdirSync(path);

var ledgers   = [ ];
var exchanges = [ ];
var pairs     = { };

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
  
  ledger.transactions.forEach(function(tx) {
    var parsed;
    
    tx        = prepareTransaction(ledger, tx);
    parsed    = Parser.parseTransaction(tx);
    exchanges = exchanges.concat(parsed.exchanges);
  });
});

console.log('# exchanges:', exchanges.length);

var hbase = new Hbase(options.hbase);
hbase.connect().then(function() {
  exchanges.forEach(function(ex) {
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
      console.log(err, resp);
    });  
  });

  var uniquePairs = Object.keys(pairs);
  console.log(uniquePairs, uniquePairs.length);
}); 





