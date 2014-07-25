var assert = require('assert');
var Account = require(__dirname+'/../../lib/models/account.js');

describe('Account sequelize model', function() {

  it('should instantiate a new account', function() {
    var account = Account.build();
    assert(account);
  });

  it('should instantiate a new account', function(done) {
    var address = 'ri3jrfoij3oi4jfo3ij4f';

    var obj = Account.build({
      address: address
    });

    obj.save()
    .error(function(err) {
      return err;
    })
    .success(function() {
      assert(obj.address.toString() === address);
      done();
    });
  });

});

