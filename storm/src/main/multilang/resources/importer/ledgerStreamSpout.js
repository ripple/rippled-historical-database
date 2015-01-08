var storm    = require('./storm');
var Spout    = storm.Spout;
var Importer = require('../import/importer');
var config   = require('../config/import.config');
var Hbase    = require('../lib/hbase/client');
var client   = new Hbase(config.get('hbase'));
var ledgerSpout;

function LedgerStreamSpout() {
  var self   = this;
  self.live  = new Importer();
  self.queue = [];
  
  //start live importer
  self.live.liveStream();
  self.live.on('ledger', function (ledger) {
    self.queue.push(ledger);
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
    ledger.transactions.push(tx.hash);
  });
  
  client.saveLedger(ledger, function(err, resp) {
    if (err) {
      self.log('unable to save ledger: ' + ledger.ledger_index);
    } else {
      self.log('ledger saved: ' + ledger.ledger_index);
    }
  }); 
};


LedgerStreamSpout.prototype.nextTuple = function(done) {
  var self = this;
  
  setTimeout(function(){

    while(self.queue.length) {
      self.processLedger(self.queue.shift());
      
      //self.emit({tuple:[ledger], id:ledger.ledger_hash}, function(taskIds){
      //  self.log(ledger.ledger_index + ' sent to task ids - ' + taskIds);
      //});
    }

    done();
  }, 100);
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
