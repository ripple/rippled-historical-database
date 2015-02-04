var Storm  = require('./lib/storm');
var config = require('../config');
var Spout  = Storm.Spout;

var LedgerStream = require('./lib/ledgerStream');
var stream       = new LedgerStream(config);
var ledgerSpout;

/**
 * LedgerStreamSpout
 */

function LedgerStreamSpout() {
  Spout.call(this); 
  this.pending = { };
};

LedgerStreamSpout.prototype = Object.create(Spout.prototype);
LedgerStreamSpout.prototype.constructor = LedgerStreamSpout;

/**
 * nextTuple
 */

LedgerStreamSpout.prototype.nextTuple = function(done) {
  var self    = this;
  var timeout = 100;
  var tx;
  var id;
  
  //process all ledgers
  while (stream.ledgers.length) {
    stream.processNextLedger(function(err, resp) {
      if (err) {
        self.log(err);        
      }
    });
  }
    
  //emit one transaction per call
  if (stream.transactions.length) {
    tx = stream.transactions.shift();  
    id = tx.ledger_index + '|' + tx.tx_index;
    
    //call the function immedately
    //if there are more transactions to process
    if (stream.transactions.length) timeout = 0;
    
    //keep the transaction around
    //until its acked
    self.pending[id] = {
      attempts : 1,
      tx       : tx
    };
    
    //emit transaction
    self.emit({tuple:[tx], id:id}, function(taskIds){
      self.log('tx: ' + id + ' sent to - ' + taskIds);
    });
  }
  
  setTimeout(done, timeout);
};

/**
 * ack
 * report a transaction as success
 * and remove it from the pending list
 */

LedgerStreamSpout.prototype.ack = function(id, done) {
  this.log('Received ack for - ' + id);
  delete this.pending[id];
  done();
};

/**
 * fail
 * report a transaction as failed
 * retry it up to 10 times
 */

LedgerStreamSpout.prototype.fail = function(id, done) {
  var self = this;

  if (++self.pending[id].attempts <= 10) {
    self.log('Received FAIL for - ' + id + ' Retrying, attempt #' + self.pending[id].attempts);
    self.emit({tuple: self.pending[id].tx, id:id}, function(taskIds) {
        self.log('tx: ' + id + ' resent to - ' + taskIds);
    });
    
  } else {
    self.log('Received FAIL for - ' + id + ' - Stopping after 10 attempts');
    delete self.pending[id];
  }
  
  done();
};

ledgerSpout = new LedgerStreamSpout();
ledgerSpout.run();
stream.start();
