var db = require('../sequelize.js');
var Sequelize = require('sequelize');
var Transaction = require('./transaction');
var LedgerTransaction = require('./ledger_transaction');
var AccountTransaction = require('./account_transaction');
var Account = require('./account');
var _ = require('underscore');
var async = require('async');


var createTransaction = function(transaction, ledger_id, account_obj, fn) {
  Transaction.createFromJSON(transaction, function(error, transaction) {
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
      }).complete(callback);
    }

    var createAccountTransaction = function(callback) {
      // Create AccountTransaction
      var account = account_obj[transaction.account.toString()];
      if (!account || !account.id) {
        throw new Error('Could not find account');
      }

      AccountTransaction.create({
        transaction_id: transaction.id,
        account_id: account.id,
        ledger_sequence: ledger_id,
        transaction_sequence: transaction.sequence
      }).complete(callback);
    }

    async.parallel([
      createLedgerTransaction,
      createAccountTransaction
      ],
      function(error, results) {
      if (error) {
        // TODO: Rollback
        fn(error);
      }
      else {
        //console.log('Created Transaction');
        results.push(transaction); // Add transaction to results
        fn(null, results);
      }
    });
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
        }).complete(function(error, ledger) {
          if (error) {
            // No need to rollback
            fn(error);
            return;
          }

          var created_objects = [ledger];

          // Create transactions
          var q = async.queue(function (transaction, callback) {
            createTransaction(transaction, ledger_id, account_obj, callback);
          }, 1);

          // assign a callback
          q.drain = function(error) {
            if (error) {
              // Rollback
              console.log('Rolling back ledger ' + ledger_id + ' and ' + created_objects.length + ' objects');

              var qu = async.queue(function (obj, callback) {
                obj.destroy().complete(callback);
              }, 1);

              qu.drain = function(error) {
                console.log('all objects have been deleted');
                fn(error);
              }

              qu.push(created_objects, function () {
                //console.log('finished deleting object');
              });
            }
            else {
              console.log('all transactions have been processed');
              fn(null, ledger);
            }
          }

          q.push(transactions, function (err, result) {
            // console.log('finished creating transaction');
            created_objects = created_objects.concat(result);
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
