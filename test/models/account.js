var assert = require('assert');
var Account = require(__dirname+'/../../lib/models/account.js');

describe('Account sequelize model', function() {

  it('should instantiate a new account', function() {
    var account = Account.build();
    assert(account);
  });

});

