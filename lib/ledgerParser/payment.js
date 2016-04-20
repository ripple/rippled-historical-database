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

  if (tx.DestinationTag) {
    payment.destination_tag = tx.DestinationTag;
  }

  if (tx.SourceTag) {
    payment.source_tag = tx.SourceTag;
  }

  if (tx.InvoiceID) {
    payment.invoice_id = tx.InvoiceID;
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

  setIssuer(payment, tx);

  payment.fee = new BigNumber (tx.Fee).dividedBy(XRP_ADJUST).toString();
  payment.ledger_index = tx.ledger_index;
  payment.tx_index = tx.tx_index;
  payment.time = tx.executed_time;
  payment.tx_hash = tx.hash;
  payment.client = tx.client;

  return payment;

  //determine the issuer
  function setIssuer() {
    var node;
    var balance;
    var prev;
    var change;
    var high;
    var low;

    // XRP has no issuer
    if (payment.currency === 'XRP') {
      return;
    }

    if (payment.source !== tx.Amount.issuer &&
        payment.destination !== tx.Amount.issuer) {
      payment.issuer = tx.Amount.issuer;
      return;
    }

    for (var i=0; i < tx.metaData.AffectedNodes.length; i++) {
      node = tx.metaData.AffectedNodes[i].CreatedNode ||
          tx.metaData.AffectedNodes[i].ModifiedNode ||
          tx.metaData.AffectedNodes[i].DeletedNode;

      if (node.LedgerEntryType !== 'RippleState') {
        continue;
      }

      if (!node.FinalFields) {
        continue;
      }

      if (node.FinalFields.HighLimit.currency !== payment.currency) {
        continue;
      }

      high = node.FinalFields.HighLimit.issuer;
      low  = node.FinalFields.LowLimit.issuer;

      // destination balance changes
      if (high === payment.destination || low === payment.destination) {
        balance = parseFloat(node.FinalFields.Balance.value);
        previous = node.PreviousFields ?
          parseFloat(node.PreviousFields.Balance.value) : 0;

        // if the balance is negative,
        // or was negative previous to this tx,
        // the lowLimit account is the issuer
        if (balance < 0 || previous < 0) {
          payment.issuer = low;

        } else {
          payment.issuer = high;
        }

        return;
      }
    }
  }
}

module.exports = Payments;
