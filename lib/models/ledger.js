var db = require('../sequelize.js');
var Sequelize = require('sequelize');
var Transaction = require('./transaction');
var LedgerTransaction = require('./ledger_transaction');
var AccountTransaction = require('./account_transaction');
var Account = require('./account');
var _ = require('underscore');


var Ledger = db.define('ledgers', {
  id:                    Sequelize.BIGINT,
  ledger_hash:           Sequelize.BLOB,
  parent_hash:           Sequelize.BLOB,
  total_coins:           Sequelize.BIGINT,
  close_time:            Sequelize.BIGINT,
  close_time_resolution: Sequelize.BIGINT,
  account_hash:          Sequelize.BLOB,
  transaction_hash:      Sequelize.BLOB,
  accepted:              Sequelize.BOOLEAN,
  closed:                Sequelize.BOOLEAN,
  close_time_estimated:  Sequelize.BOOLEAN,
  close_time_human:      Sequelize.DATE

  //parent_close_time:     Sequelize.BIGINT,
  //close_flags:           Sequelize.BIGINT,
  //state_hash:            Sequelize.BLOB,
}, {
  timestamps: false,
  underscored: true,

  classMethods: {
    createFromJSON: function(json, fn) {
      var transactions = json.transactions;
      var ledger_id = json.ledger_index;

      // var transaction_functions = _.map(transactions, function(json) { return createTransaction(json, callback)});

      // async.waterfall(transaction_functions, function (err, result) {
      //    // result now equals 'done'
      //    console.log('OIWJEFOIWJEF', err, result);
      // });


      // Find all accounts
      var addresses = _.map(transactions, function(transaction) { return transaction.Account });
      addresses = _.uniq(addresses);

      console.log('11111111111111', addresses);

      Account.findOrCreate(addresses, function(error, accounts) {
        var keys = _.map(accounts, function(account) { return account.address.toString() });
        var account_obj = _.object(keys, accounts);

        for (var i = 0; i < transactions.length; i++) {
          var transaction = transactions[i];
          Transaction.createFromJSON(transaction, function(error, transaction) {
            if (error) {
              fn(error);
              return;
            }
            console.log('Created Transaction');

            LedgerTransaction.create({
              transaction_id: transaction.id,
              ledger_id: ledger_id,
              transaction_sequence: transaction.sequence
            }).complete(function(error, ledger_transaction) {
              if (error) {
                fn(error);
                return;
              }
              // TODO: Error checking
              console.log('Created LedgerTransaction');
            });


            var account = account_obj[transaction.account.toString()];
            // var address = transaction.account.toString();
            // for(var j=0;j < accounts.length; j++) {
            //   console.log('TYPE: ' + accounts[j].address.toString() + ' type: ' + typeof accounts[j].address.toString());
            //   if (accounts[j].address.toString() === address) {
            //     account = accounts[j];
            //   }
            // }
            if (!account || !account.id) {
              console.log('ACCOUNT', account);
              console.log('WOIEJOWIEJFO');
              throw new Error('error');
            }

            AccountTransaction.create({
              transaction_id: transaction.id,
              account_id: account.id,
              ledger_sequence: ledger_id,
              transaction_sequence: transaction.sequence
            }).complete(function(error, account_transaction) {
              if (error) {
                fn(error);
                return;
              }
              // TODO: Error checking
              console.log('Created AccountTransaction');
            });
          });
        }
      });

      Ledger.create({
        id: json.ledger_index,
        ledger_hash: json.ledger_hash,
        parent_hash: json.parent_hash,
        total_coins: json.total_coins,
        close_time: json.close_time,
        close_time_resolution: json.close_time_resolution,
        account_hash: json.account_hash,
        transaction_hash: json.transaction_hash,
        accepted: json.accepted,
        closed: json.closed,
        close_time_estimated: json.close_time_estimated,
        close_time_human: json.close_time_human

        //prev_closing_time: json., // Not in the json response
        //close_flags: json., // Not in the json response
        //state_hash: json. // Not in the json response
      }).complete(fn);

      // TODO: Save ledger transactions
    },

    setOfSavedLedgers: function(fn) {
      Ledger.findAll({
        attributes: ['id']
      }).complete(fn);
    }
  }
});

module.exports = Ledger;
