var storm = require('./storm');
var BasicBolt = storm.BasicBolt;

function SaveLedgerBolt() {
  BasicBolt.call(this);
}

SaveLedgerBolt.prototype = Object.create(BasicBolt.prototype);
SaveLedgerBolt.prototype.constructor = SaveLedgerBolt;

SaveLedgerBolt.prototype.process = function(tup, done) {
  var self   = this;
  var ledger = tup.values[0];
  self.log('saving ledger: ' + ledger.ledger_index + ' ' + tup.id);
  done();
}

new SaveLedgerBolt().run();
