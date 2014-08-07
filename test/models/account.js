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


  it('should find and create', function(done) {
    var addresses = [
    'rhQMNd4yrCYnyGbAvSUieeundgtEXacoBT',
    'rM3X3QSr8icjTGpaF52dozhbT2BZSXJQYM',
    'rM3X3QSr8icjTGpaF52dozhbT2BZSXJQYB',
    'rM3X3QSr8icjTGpaF52dozhbT2BZSXJQYA',
    'rM3X3QSr8icjTGpaF52dozhbT2BZSXJQYC',
    'rM3X3QSr8icjTGpaF52dozhbT2BZSXJQYQ'
    ];

    Account.findOrCreate(addresses, function(error, accounts){
      if (error) return error;
      //console.log(accounts);
      done();
    });
  });

});

