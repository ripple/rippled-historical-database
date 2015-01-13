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
    //self.processStreams(tx, data, tup.id),
    
  ]).nodeify(function(err, resp){
    
    if (err) {
      self.log(err);
      self.fail(tup);
      
    } else {
      self.ack(tup);
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

TransactionBolt.prototype.processStreams = function (data, id) {
  var self = this;
  
  if (data.payment) {
    self.emit({
      tuple         : [data.payment], 
      anchorTupleId : id,
      stream        : 'payments'
    }, 
    function(taskIds) {
        self.log('payment sent to task ids - ' + taskIds);
    });
  }
};

bolt = new TransactionBolt();
bolt.run();
