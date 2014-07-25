var assert = require('assert');
var Transaction = require(__dirname+'/../../lib/models/transaction.js');

describe('Transaction sequelize model', function() {

  it('should instantiate a new transaction', function() {
    var transaction = Transaction.build();
    assert(transaction);
  });

  it('should create a new transaction', function(done) {
    var opts = {
      hash: '329485u230945802394852',
      type: 'Payment',
      //from_account: 1, // TODO: Create account first
      from_sequence: 1,
      ledger_sequence: 1,
      status: 1,
      raw: 'testjweoifjwoiefj',
      meta:  'oijaoweifjwefij'
    };

    var obj = Transaction.build(opts);

    obj.save()
    .error(function(err) {
      return err;
    })
    .success(function() {
      assert(obj.hash.toString() === opts.hash);
      assert(obj.type === opts.type);
      //assert(obj.from_account == opts.from_account);
      assert(obj.from_sequence == opts.from_sequence);
      assert(obj.ledger_sequence == opts.ledger_sequence);
      assert(obj.status == opts.status);
      assert(obj.raw.toString() === opts.raw);
      assert(obj.meta.toString() === opts.meta);
      done();
    });
  });

});

