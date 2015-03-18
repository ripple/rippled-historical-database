var Amount     = require('ripple-lib').Amount;
var BigNumber  = require('bignumber.js');
var XRP_ADJUST = 1000000.0;
var parseBalanceChanges   = require('ripple-lib-transactionparser').parseBalanceChanges;
var parseOrderBookChanges = require('ripple-lib-transactionparser').parseOrderBookChanges;

var Payments = function (tx) {
  var payment = { };
  var amount;
  var balanceChanges;
  var sourceBalanceChanges;

  if (tx.metaData.TransactionResult !== 'tesSUCCESS') {
    return null;
  }

  if (tx.TransactionType !== 'Payment') {
    return null;
  }

  //ignore 'convert' payments
  if (tx.Account === tx.Destination) {
    return null;
  }

  //parse balance changes
  balanceChanges = parseBalanceChanges(tx.metaData);

  payment.source      = tx.Account;
  payment.destination = tx.Destination;
  payment.source_balance_changes      = [];
  payment.destination_balance_changes = balanceChanges[tx.Destination];

  balanceChanges[tx.Account].forEach(function(change) {
    if (change.currency === 'XRP') {
      var fee      = new BigNumber (tx.Fee).dividedBy(XRP_ADJUST).negated();
      change.value = new BigNumber(change.value).minus(fee).toString();
    }

    if (change.value !== '0') {
      payment.source_balance_changes.push(change);
    }
  });

  if (tx.DestionationTag) {
    payment.destination_tag = tx.DestionationTag;
  }

  if (tx.SourceTag) {
    payment.source_tag = tx.SourceTag;
  }

  //destination amount and currency
  if (typeof tx.Amount === 'object') {
    payment.currency = tx.Amount.currency;
    payment.amount   = tx.Amount.value;

  } else {
    payment.currency = 'XRP';
    payment.amount   = new BigNumber(tx.Amount).dividedBy(XRP_ADJUST).toString();
  }

  //delivered amount fields
  amount = tx.metaData.DeliveredAmount || tx.Amount;
  if (typeof amount === 'object') {
    payment.delivered_amount = amount.value;

  } else {
    payment.delivered_amount = new BigNumber(amount).dividedBy(XRP_ADJUST).toString();
  }

  //max amount
  if (typeof tx.SendMax === 'object') {
    payment.max_amount      = tx.SendMax.value;
    payment.source_currency = tx.SendMax.currency;

  } else if (tx.SendMax) {
    payment.max_amount      = new BigNumber(tx.SendMax).dividedBy(XRP_ADJUST).toString();
    payment.source_currency = 'XRP';
  }

  if (!payment.source_currency && payment.source_balance_changes.length) {
    payment.source_currency = payment.source_balance_changes[0].currency;
  }

  payment.fee          = new BigNumber (tx.Fee).dividedBy(XRP_ADJUST).toString();
  payment.ledger_index = tx.ledger_index;
  payment.tx_index     = tx.tx_index;
  payment.time         = tx.executed_time;
  payment.tx_hash      = tx.hash;
  payment.client       = tx.client;

  return payment;
}

module.exports = Payments;
