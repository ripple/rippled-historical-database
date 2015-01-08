var storm = require('./storm');
var BasicBolt = storm.BasicBolt;

function TransactionBolt() {
  BasicBolt.call(this);
}

TransactionBolt.prototype = Object.create(BasicBolt.prototype);
TransactionBolt.prototype.constructor = TransactionBolt;

TransactionBolt.prototype.process = function(tup, done) {
  var self = this;
  var tx   = tup.values[0];
  self.log('transaction: ' + tx.hash);
  done();
}

new TransactionBolt().run();