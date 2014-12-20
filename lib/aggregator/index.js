var parser   = require('../ledgerParser');
var exchange = require('./exchange');
var log      = require('../log')('aggregator');
var moment   = require('moment');

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

      var exchanges = parser.offersExercised(transaction);


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