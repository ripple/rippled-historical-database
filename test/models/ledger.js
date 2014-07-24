var assert = require('assert');
var Ledger = require(__dirname+'/../../lib/models/ledger.js');

describe('Ledger sequelize model', function() {

  it('should instantiate a new ledger', function() {
    var ledger = Ledger.build();
    assert(ledger);
  });

});

