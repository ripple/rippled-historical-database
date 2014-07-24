var assert = require('assert');
var AccountTransaction = require(__dirname+'/../../lib/models/account_transaction.js');

describe('AccountTransaction sequelize model', function() {

  it('should instantiate a new account', function() {
    var accountTransaction = AccountTransaction.build();
    assert(accountTransaction);
  });

});

