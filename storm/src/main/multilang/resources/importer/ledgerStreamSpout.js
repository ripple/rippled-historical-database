var storm    = require('./storm');
var Spout    = storm.Spout;
var Importer = require('../import/importer');
var config   = require('../config/import.config');
var utils    = require('../lib/utils');
var Hbase    = require('../lib/hbase/client');
var client   = new Hbase(config.get('hbase'));
var ledgerSpout;

/**
 * LedgerStreamSpout
 */

function LedgerStreamSpout() {
  var self   = this;
  self.live  = new Importer();
  self.ledgers      = [];
  self.transactions = [];
  
  //start live importer
  self.live.liveStream();
  self.live.on('ledger', function (ledger) {
    self.ledgers.push(ledger);
  });
  
  self.runningTupleId = 0;
  self.pending        = {};
  Spout.call(this); 
};

LedgerStreamSpout.prototype = Object.create(Spout.prototype);
LedgerStreamSpout.prototype.constructor = LedgerStreamSpout;

/**
 * processLedger
 */

LedgerStreamSpout.prototype.processLedger = function (ledger) {
  var self = this;
  var transactions = ledger.transactions;
  
  self.log('Ledger: ' + ledger.ledger_index + ' - # transactions: ' + transactions.length);

  //save transaction hashes 
  ledger.transactions = [];  
  transactions.forEach(function(tx) {
    var prepared = self.prepareTransaction(ledger, tx);
    ledger.transactions.push(tx.hash);  
    
    if (prepared) {
      self.transactions.push(prepared);
    
    } else {
      //TODO: log these specially as it means the transaction 
      //will not be saved
      self.log('error preparing tx: ' + ledger.ledger_index + ' ' + tx.hash);
    }
  });
  
  //save ledger to hbase
  client.saveLedger(ledger, function(err, resp) {
    if (err) {
      self.log('unable to save ledger: ' + ledger.ledger_index);
    } else {
      self.log('ledger saved: ' + ledger.ledger_index);
    }
  }); 
};

/**
 * prepareTransaction
 */

LedgerStreamSpout.prototype.prepareTransaction = function (ledger, tx) {
  var meta = tx.metaData;
  delete tx.metaData;
    
  try {
    tx.raw  = utils.toHex(tx);
    tx.meta = utils.toHex(meta);

  } catch (e) {
    console.log(e, tx.ledger_index, tx.hash);
    return;
  }
    
  tx.metaData        = meta;
  tx.ledger_hash     = ledger.ledger_hash;
  tx.ledger_index    = ledger.ledger_index;
  tx.executed_time   = ledger.close_time;
  tx.tx_index        = tx.metaData.TransactionIndex;
  tx.tx_result       = tx.metaData.TransactionResult;
  
  return tx;
};


LedgerStreamSpout.prototype.nextTuple = function(done) {
  var self    = this;
  var timeout = 100;
  var tx;
  
  //process all ledgers
  while (self.ledgers.length) {
    self.processLedger(self.ledgers.shift());
  }
    
  //emit one transaction per call
  if (self.transactions.length) {
    tx = self.transactions.shift();  
    if (self.transactions.length) timeout = 0;
  }
  
  setTimeout(function(){
    var id;
    
    //emit transaction
    if (tx) {
      id = tx.ledger_index + '|' + tx.tx_index;
      self.emit({tuple:[tx], id:id}, function(taskIds){
        self.log('tx: ' + id + ' sent to - ' + taskIds);
      });
    }
    
    done();
  }, 10);
};

LedgerStreamSpout.prototype.ack = function(id, done) {
  this.log('Received ack for - ' + id);
  //delete this.pending[id];
  done();
};

LedgerStreamSpout.prototype.fail = function(id, done) {
  var self = this;
  this.log('Received fail for - ' + id + '. Retrying.');
  //this.emit({tuple: this.pending[id], id:id}, function(taskIds) {
  //    self.log(self.pending[id] + ' sent to task ids - ' + taskIds);
  //});
  done();
};

ledgerSpout = new LedgerStreamSpout();
ledgerSpout.run();

client.connect().then(function(connected) {
  console.log("HBASE connected:", connected);
  if (connected) {
    ledgerSpout.log('HBASE connected');
  } else {
    ledgerSpout.log('unable to connect to HBASE');
  }
}); 
