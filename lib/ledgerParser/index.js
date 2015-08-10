var SerializedObject = require('ripple-lib').SerializedObject;
var binformat        = require('ripple-lib').binformat;
var utils            = require('../utils');

var EPOCH_OFFSET = 946684800;
var LI_PAD       = 12;
var I_PAD        = 5;
var E_PAD        = 3;
var S_PAD        = 12;
var Parser       = { };

var TX_TYPES   = { };
var TX_RESULTS = { };

Object.keys(binformat.tx).forEach(function(key) {
  TX_TYPES[key] = binformat.tx[key][0];
});

Object.keys(binformat.ter).forEach(function(key) {
  TX_RESULTS[key] = binformat.ter[key];
});

Parser.affectedAccounts = require('./affectedAccounts');
Parser.exchanges        = require('./exchanges');
Parser.offers           = require('./offers');
Parser.balanceChanges   = require('./balanceChanges');
Parser.accountsCreated  = require('./accountsCreated');
Parser.memos            = require('./memos');
Parser.payment          = require('./payment');
Parser.fromClient       = require('./fromClient');

Parser.parseLedger = function(ledger) {
  var data = {
    ledger           : null,
    transactions     : [],
    affectedAccounts : [],
    accountsCreated  : [],
    exchanges        : [],
    offers           : [],
    balanceChanges   : [],
    payments         : [],
    memos            : [],
    valueMoved       : []
  }

  var transactions = ledger.transactions;

  //note: this will only work until 2030
  if (ledger.close_time < EPOCH_OFFSET) {
    ledger.close_time   = ledger.close_time + EPOCH_OFFSET;
  }

  ledger.transactions = [];

  transactions.forEach(function(transaction) {
    ledger.transactions.push(transaction.hash);
    var meta = transaction.metaData;
    var payment;

    delete transaction.metaData;

    try {
      transaction.raw  = utils.toHex(transaction);
      transaction.meta = utils.toHex(meta);

    } catch (e) {
      console.log(e, transaction.ledger_index, transaction.hash);
      return;
    }

    transaction.metaData        = meta;
    transaction.ledger_hash     = ledger.ledger_hash;
    transaction.ledger_index    = ledger.ledger_index;
    transaction.executed_time   = ledger.close_time;
    transaction.tx_index        = transaction.metaData.TransactionIndex;
    transaction.tx_result       = transaction.metaData.TransactionResult;

    //set 'client' string, if its present in a memo
    transaction.client = Parser.fromClient(transaction);

    data.transactions.push(transaction);

    data.exchanges        = data.exchanges.concat(Parser.exchanges(transaction));
    data.offers           = data.offers.concat(Parser.offers(transaction));
    data.balanceChanges   = data.balanceChanges.concat(Parser.balanceChanges(transaction));
    data.accountsCreated  = data.accountsCreated.concat(Parser.accountsCreated(transaction));
    data.affectedAccounts = data.affectedAccounts.concat(Parser.affectedAccounts(transaction));
    data.memos            = data.memos.concat(Parser.memos(transaction));

    //parse payment
    payment = Parser.payment(transaction);
    if (payment) {
      data.payments.push(payment);
    }
  });

  data.ledger = ledger;
  return data;
};

/**
 * parseTransaction
 * Parse a single transaction
 */

Parser.parseTransaction = function (tx) {
  var data = { };
  var payment;

  data.exchanges        = Parser.exchanges(tx);
  data.offers           = Parser.offers(tx);
  data.balanceChanges   = Parser.balanceChanges(tx);
  data.accountsCreated  = Parser.accountsCreated(tx);
  data.affectedAccounts = Parser.affectedAccounts(tx);
  data.memos            = Parser.memos(tx);
  data.payments         = [];
  payment               = Parser.payment(tx);

  if (payment) {
    data.payments.push(payment);
  }

  return data;
};

//Convert json to binary/hex to store as raw data
function toHex(input){
  return new SerializedObject.from_json(input).to_hex();
}


function pad(num, size) {
  var s = num+"";
  if (!size) size = 10;
  while (s.length < size) s = "0" + s;
  return s;
}

module.exports = Parser;
