var config   = require('../../config/import.config');
var Logger   = require('../../storm/multilang/resources/src/lib/modules/logger');
var Hbase    = require('../../storm/multilang/resources/src/lib/hbase-client');
var Parser   = require('../../storm/multilang/resources/src/lib/modules/ledgerParser');
var moment   = require('moment');
var hbaseOptions = config.get('hbase');
var hbase;
var exchange;

var log = new Logger({
  scope : 'aggregator',
  level : config.get('logLevel') || 0,
  file  : config.get('logFile')
});

hbaseOptions.logLevel = 2;
hbase    = new Hbase(hbaseOptions);
exchange = require('./exchange')(hbase);
hbase.connect();

var EPOCH_OFFSET = 946684800;

var Aggregator = function () {
  
  var self = this;
  
  self.digestLedger = function (ledger) {
    var pairs = {};
    var time  = moment.utc(ledger.close_time_timestamp);

    //get all pairs that need to be updated
    ledger.transactions.forEach(function(transaction) {

      transaction.ledger_index   = ledger.ledger_index;
      transaction.executed_time  = time.unix();
      transaction.tx_index       = transaction.metaData.TransactionIndex;

      var exchanges = Parser.exchanges(transaction);


      exchanges.forEach(function(ex) {
        var base    = ex.base.currency;
        var counter = ex.counter.currency;
        var key;

        if (base != 'XRP')    base    += '.' + ex.base.issuer;
        if (counter != 'XRP') counter += '.' + ex.counter.issuer;

        if (base < counter) {
          key = base + ':' + counter;
        } else {
          key = counter + ':' + base;
        }

        pairs[key] = true;
      });
    });
    
    console.log(pairs, time.format());
    aggregatePairs(pairs, time);
  };
  
  function aggregatePairs (pairs, time) {
    var keys = Object.keys(pairs);
    keys.forEach(function(key) {
      exchange.cacheIntervals(key, time);
    });
  }
  
  return this;
};


module.exports = new Aggregator();