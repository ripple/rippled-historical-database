var assert = require('assert');
var Transaction = require(__dirname+'/../../lib/models/transaction.js');

describe('Transaction sequelize model', function() {

  it('should instantiate a new transaction', function() {
    var transaction = Transaction.build();
    assert(transaction);
  });

});

