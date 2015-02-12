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
    stream.processNextLedger(function(err, ledger) {
      if (err) {
        self.log(err);        
        return;
      }
      
      self.pending[ledger.ledger_index] = {
        ledger       : ledger,
        transactions : { },
        acks         : [ ]
      };
      
      self.log('# pending ledgers: ' + Object.keys(self.pending).length);
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
    self.pending[tx.ledger_index].transactions[tx.tx_index] = {
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
  var self  = this;
  var parts = id.split('|');
  var data  = self.pending[parts[0]];
  
  self.log('Received ack for - ' + id);
  data.acks.push(parts[1]);
  
  //if we've acked all transactions, save the ledger
  if (data.acks.length == data.ledger.transactions.length) {
    stream.hbase.saveLedger(data.ledger, function(err, resp) {
      if (err) {
        self.log.error(err);
        self.log('unable to save ledger: ' + data.ledger.ledger_index);
      
      } else {
        self.log('ledger saved: ' + data.ledger.ledger_index);
      }
      
      //remove the pending ledger
      delete self.pending[parts[0]];
      done();
    });    
  
  } else {
    done();
  }
};

/**
 * fail
 * report a transaction as failed
 * retry it up to 10 times
 */

LedgerStreamSpout.prototype.fail = function(id, done) {
  var self   = this;
  var parts  = id.split('|');
  var data   = this.pending[parts[0]];
  var txData = data ? data.transactions[parts[1]] : null;  
  
  if (!data) {
    self.log('Received FAIL for - ' + id + ' Stopping, ledger failed');
    
  } else if (++txData.attempts <= 5) {
    self.log('Received FAIL for - ' + id + ' Retrying, attempt #' + txData.attempts);
    self.emit({tuple: [txData.tx], id:id}, function(taskIds) {
        self.log('tx: ' + id + ' resent to - ' + taskIds);
    });
    
  } else {
    self.log('Received FAIL for - ' + id + ' - Stopping after 5 attempts');
    
    //remove the failed ledger
    delete self.pending[parts[0]];
  }
  
  done();
};

ledgerSpout = new LedgerStreamSpout();
ledgerSpout.run();
stream.start();
