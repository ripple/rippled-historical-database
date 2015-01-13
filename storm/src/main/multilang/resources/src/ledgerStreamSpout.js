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
        console.log(err);
        //TODO: log these as failed imports
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

    //emit transaction
    self.emit({tuple:[tx], id:id}, function(taskIds){
      self.log('tx: ' + id + ' sent to - ' + taskIds);
    });
  }
  
  console.log(timeout);
  setTimeout(done, timeout);
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
stream.start();
