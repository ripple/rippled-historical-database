var storm    = require('./storm');
var Spout    = storm.Spout;
var Importer = require('../import/importer');

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

LedgerStreamSpout.prototype.nextTuple = function(done) {
  var self = this;
  
  setTimeout(function(){
    var ledger;
    
    while(self.queue.length) {
      ledger = self.queue.shift();
      self.log('new ledger: ' + ledger.ledger_index);
      self.emit({tuple:[ledger], id:ledger.ledger_hash}, function(taskIds){
        self.log(ledger.ledger_index + ' sent to task ids - ' + taskIds);
      });
    }

    done();
  }, 100);
}

LedgerStreamSpout.prototype.ack = function(id, done) {
  this.log('Received ack for - ' + id);
  delete this.pending[id];
  done();
}

LedgerStreamSpout.prototype.fail = function(id, done) {
  var self = this;
  this.log('Received fail for - ' + id + '. Retrying.');
  this.emit({tuple: this.pending[id], id:id}, function(taskIds) {
      self.log(self.pending[id] + ' sent to task ids - ' + taskIds);
  });
  done();
}


new LedgerStreamSpout().run();