var assert = require('assert');
var AccountTransaction = require(__dirname+'/../../lib/models/account_transaction.js');

describe('AccountTransaction sequelize model', function() {

  it('should instantiate a new account', function() {
    var accountTransaction = AccountTransaction.build();
    assert(accountTransaction);
  });

  // it('should create a new account transaction', function(done) {
  //   var opts = {
  //     transaction_id: 1,
  //     account_id: 1,
  //     ledger_sequence: 1,
  //     transaction_sequence: 1
  //   }

  //   var obj = AccountTransaction.build(opts);

  //   obj.save()
  //   .error(function(err) {
  //     return err;
  //   })
  //   .success(function() {
  //     assert(obj.transaction_id == opts.transaction_id);
  //     assert(obj.account_id == opts.account_id);
  //     assert(obj.ledger_sequence == opts.ledger_sequence);
  //     assert(obj.transaction_sequence == opts.transaction_sequence);
  //     done();
  //   });
  // });

});

