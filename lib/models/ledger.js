var db = require('../sequelize.js');
var Sequelize = require('sequelize');
var Transaction = require('./transaction');
var LedgerTransaction = require('./ledger_transaction');
var AccountTransaction = require('./account_transaction');
var Account = require('./account');
var _ = require('underscore');
var async = require('async');


var createTransaction = function(transaction, ledger_id, account_obj, t, fn) {
  Transaction.createFromJSON(transaction, t, function(error, transaction) {
    if (error) {
      fn(error);
      return;
    }

    var createLedgerTransaction = function(callback) {
      // Create LedgerTransaction
      LedgerTransaction.create({
        transaction_id: transaction.id,
        ledger_id: ledger_id,
        transaction_sequence: transaction.sequence
      }, { transaction: t }).complete(callback);
    }

    var createAccountTransaction = function(callback) {
      // Create AccountTransaction
      var account = account_obj[transaction.account.toString()];
      if (!account || !account.id) {
        callback(new Error('Could not find account'));
        return;
      }

      AccountTransaction.create({
        transaction_id: transaction.id,
        account_id: account.id,
        ledger_sequence: ledger_id,
        transaction_sequence: transaction.sequence
      }, { transaction: t }).complete(callback);
    }

    async.parallel([
      createLedgerTransaction,
      createAccountTransaction
      ],
      function(error, results) {
        fn(error);
      }
    );
  });
}


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

      // Find all accounts
      var addresses = _.map(transactions, function(transaction) { return transaction.Account });
      addresses = _.uniq(addresses);

      Account.findOrCreate(addresses, function(error, accounts) {
        if (error) {
          fn(error);
          return;
        }
        var keys = _.map(accounts, function(account) { return account.address.toString() });
        var account_obj = _.object(keys, accounts);

        db.transaction(function(t) {
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
          }, { transaction: t }).complete(function(error, ledger) {
            if (error) {
              // No need to rollback
              fn(error);
              return;
            }

            // Create transactions
            var q = async.queue(function (transaction, callback) {
              createTransaction(transaction, ledger_id, account_obj, t, callback);
            }, 1);

            // assign a callback
            q.drain = function(error) {
              if (error) {
                // Rollback
                console.log('Rolling back ledger ' + ledger_id);
                t.rollback().complete(function() { fn(error) });
              }
              else {
                console.log('all transactions have been processed');
                t.commit().complete(function() { fn(null, ledger) });
              }
            }

            q.push(transactions, function (err, result) {
              // console.log('finished creating transaction');
            });
          });
        });
      });
    },

    setOfSavedLedgers: function(fn) {
      Ledger.findAll({
        attributes: ['id']
      }).complete(fn);
    }
  }
});

module.exports = Ledger;
