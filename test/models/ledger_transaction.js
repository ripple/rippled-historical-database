var assert = require('assert');
var LedgerTransaction = require(__dirname+'/../../lib/models/ledger_transaction.js');

describe('LedgerTransaction sequelize model', function() {

  it('should instantiate a new ledgerTransaction', function() {
    var ledgerTransaction = LedgerTransaction.build();
    assert(ledgerTransaction);
  });

});

