var assert = require('assert');
var Transaction = require(__dirname+'/../../lib/models/transaction.js');

describe('Transaction sequelize model', function() {

  it('should instantiate a new transaction', function() {
    var transaction = Transaction.build();
    assert(transaction);
  });

  it.skip('should create a new transaction', function(done) {
    var opts = {
      Account: 'rEqSQFMsmMmhx8tGqmSEhXZ8KjBdkW6Qbc',
      Amount: '20000000000',
      Destination: 'rhbaHwBJCm1vxvEtR3iPWhHb99D4tjMqkM',
      Fee: '12',
      Flags: 0,
      Sequence: 7,
      SigningPubKey: '02CEAD9CD2AD131F309A4C1CB6FE3690C2719A20DD96E2EC7E3F831C2781650431',
      TransactionType: 'Payment',
      TxnSignature: '30450220741AF7C1C8702F0A8CB73A3609F5ECAE5DDECBC99BE713D6770C6896134003830221008C829277C4AC143CA810B1762261FD88E2C3B0D787BD086A43C4962658A0094F',
      hash: 'C72A05BD8124CDDB0288C556EA466B278F77A6BBC35EEAF29F536A83FE4CD5D6'
      //metaData: [Object]
    };

    var obj = Transaction.createFromJSON(opts, null, function(error, transaction) {
      assert(!error && transaction);
      done();
    });
  });
});

