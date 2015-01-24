var config    = require('../config');
var Promise   = require('bluebird');
var Storm     = require('./lib/storm');
var Parser    = require('./lib/modules/ledgerParser'); 
var Hbase     = require('./lib/hbase-client');
var BasicBolt = Storm.BasicBolt;
var bolt;

function TransactionBolt() {
  config.hbase.logLevel = config.logLevel;
  config.hbase.logFile  = config.logFile;
  
  this.hbase = new Hbase(config.hbase);

  //establish connection to hbase
  this.hbase.connect(); 
  
  BasicBolt.call(this);
}

TransactionBolt.prototype = Object.create(BasicBolt.prototype);
TransactionBolt.prototype.constructor = TransactionBolt;

TransactionBolt.prototype.process = function(tup, done) {
  var self = this;
  var tx   = tup.values[0];
  var parsed;
  
  self.log('transaction: ' + tx.hash);
  
  //parse transaction
  parsed = {
    data        : Parser.parseTransaction(tx),
    ledgerIndex : tx.ledger_index,
    txIndex     : tx.tx_index
  };
    
  Promise.all([
    
    //save transaction
    self.saveTransaction(tx),
  
    //save parsed data
    self.saveParsedData(parsed),

    //emit to aggregations
    self.processStreams(parsed, tup.id),
    
  ]).nodeify(function(err, resp){
    
    if (err) {
      self.log(err);
      self.fail(tup);
      
    } else {
      //self.ack(tup);
      done();
    }
    
  });
};

/**
 * saveTransaction
 */

TransactionBolt.prototype.saveTransaction = function (tx) {
  var self = this;
  var id   = tx.ledger_index + '|' + tx.tx_index; 
  
  return new Promise (function(resolve, reject) {
    self.hbase.saveTransaction(tx, function(err, resp) {

      if (err) {
        self.log('unable to save transaction: ' + id + ' ' + tx.hash);
        reject(err);

      } else {
        self.log('transaction saved: ' + id);
        resolve();
      }
    });  
  });
};


/**
 * saveParsedData
 */

TransactionBolt.prototype.saveParsedData = function (parsed) {
  var self = this;
  var id   = parsed.ledgerIndex + '|' + parsed.txIndex;
  
  return new Promise (function(resolve, reject) {
    self.hbase.saveParsedData(parsed, function(err, resp) {
      if (err) {
        self.log('unable to save parsedData: ' + id);
        reject(err);

      } else {
        self.log('parsed data saved: ' + id);
        resolve();
      }
    });  
  });
};

/**
 * processStreams
 */

TransactionBolt.prototype.processStreams = function (parsed, id) {
  var self = this;
  
  return new Promise (function(resolve, reject) {
    
    //self.log(parsed.data.exchanges.length);
    //self.log(parsed.data.payments.length);
    //self.log(parsed.data.balance_changes.length);
    //self.log(parsed.data.accounts_created.length);
    
    parsed.data.exchanges.forEach(function(exchange) {
      var pair = exchange.base.currency + 
          (exchange.base.issuer ? "." + exchange.base.issuer : '') +
          '/' + exchange.counter.currency + 
          (exchange.counter.issuer ? "." + exchange.counter.issuer : '');
      
      self.emit({
        tuple         : [exchange, pair], 
        anchorTupleId : id,
        stream        : 'exchangeAggregation'
      }, 
      function(taskIds) {
          self.log('exchange sent to task ids - ' + taskIds);
      });  
    });

    /*
    parsed.data.payments.forEach(function(payment) { 
      self.emit({
        tuple         : [payment], 
        anchorTupleId : id,
        stream        : 'paymentAggregation'
      }, 
      function(taskIds) {
          self.log('payment sent to task ids - ' + taskIds);
      });
    });

    parsed.data.balanceChanges.forEach(function(change) { 
      self.emit({
        tuple         : [change], 
        anchorTupleId : id,
        stream        : 'balanceChangeAggregation'
      }, 
      function(taskIds) {
          self.log('balance_change sent to task ids - ' + taskIds);
      });
    });  

    parsed.data.accountsCreated.forEach(function(account) { 
      self.emit({
        tuple         : [account], 
        anchorTupleId : id,
        stream        : 'accountsCreatedAggregation'
      }, 
      function(taskIds) {
          self.log('account_created sent to task ids - ' + taskIds);
      });
    });
    
    */
    self.log("done: " + parsed.ledgerIndex + "|" + parsed.txIndex);
    resolve();
  });
};

bolt = new TransactionBolt();
bolt.run();
