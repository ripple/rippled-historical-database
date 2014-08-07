var db = require('./sequelize.js').db;
var Transaction = require('./models/transaction');
var Ledger = require('./models/ledger');
var AccountTransaction = require('./models/account_transaction');
var LedgerTransaction = require('./models/ledger_transaction')
var Account = require('./models/account');
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

    async.parallelLimit([
      createLedgerTransaction,
      createAccountTransaction
      ],
      1,
      function(error, results) {
        fn(error);
      }
    );
  });
}

function DataParser() {
}

DataParser.parseAndSaveData = function(json, fn) {
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
      Ledger.createFromJSON(json, t, function(error, ledger) {
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
            //console.log('all transactions have been processed');
            t.commit().complete(function() { fn(null, ledger) });
          }
        }

        q.push(transactions, function (err, result) {
          // console.log('finished creating transaction');
        });
      });
    });
  });
}

module.exports = DataParser;
