var storm     = require('./storm');
var Parser    = require('../lib/ledgerParser'); 
var config    = require('../config/import.config');
var Hbase     = require('../lib/hbase/client');
var client    = new Hbase(config.get('hbase'));
var BasicBolt = storm.BasicBolt;
var bolt;

function TransactionBolt() {
  BasicBolt.call(this);
}

TransactionBolt.prototype = Object.create(BasicBolt.prototype);
TransactionBolt.prototype.constructor = TransactionBolt;

TransactionBolt.prototype.process = function(tup, done) {
  var self = this;
  var tx   = tup.values[0];
  var data;
  
  
  self.log('transaction: ' + tx.hash);
  
  //parse transaction
  data = self.parseTransaction(tx);
  
  //save transaction
  self.saveTransaction(tx);
  
  //save parsed data
  self.saveParsedData(data);

  //emit to aggregations
  self.processStreams(data, tup.id);
  
  done();
};

/**
 * saveTransaction
 */

TransactionBolt.prototype.saveTransaction = function (tx) {
  var self = this;
  var id   = tx.ledger_index + '|' + tx.tx_index;
  
  client.saveTransaction(tx, function(err, resp) {

    if (err) {
      self.log('unable to save transaction: ' + id + ' ' + tx.hash);
      //TODO fail
      
    } else {
      self.log('transaction saved: ' + id);
    }
  });
};

/**
 * parseTransaction
 */

TransactionBolt.prototype.parseTransaction = function (tx) {
  var data = { };
  
  data.exchanges        = Parser.exchanges(tx);
  data.balanceChanges   = Parser.balanceChanges(tx);
  data.accountsCreated  = Parser.accountsCreated(tx);
  data.affectedAccounts = Parser.affectedAccounts(tx); 
  data.memos            = Parser.memos(tx); 
  data.payment          = Parser.payment(tx);
  
  return data;
};

/**
 * saveParsedData
 */

TransactionBolt.prototype.saveParsedData = function (data) {
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

client.connect().then(function(connected) {
  console.log("HBASE connected:", connected);
  if (connected) {
    bolt.log('HBASE connected');
  } else {
    bolt.log('unable to connect to HBASE');
  }
}); 