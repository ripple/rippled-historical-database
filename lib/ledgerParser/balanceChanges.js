var BigNumber  = require('bignumber.js');
var XRP_ADJUST = 1000000.0;

/**
 * OffersExercised;
 * parse a single transaction to extract
 * all offers exercised
 */

var BalanceChanges = function (tx) {
  var list = [];

  if (tx.metaData.TransactionResult.indexOf('tec') !== 0 &&
      tx.metaData.TransactionResult !== 'tesSUCCESS') {
    return list;
  }

  tx.metaData.AffectedNodes.forEach( function(affNode, i) {
    var node = affNode.ModifiedNode || affNode.CreatedNode || affNode.DeletedNode;

    if (!node) {
      return;
    }

    node.nodeIndex = i;
    if (node.LedgerEntryType === "AccountRoot" ) {
      parseAccountRoot(node);

    } else if (node.LedgerEntryType === "RippleState") {
      parseRippleState(node);
    }
  });

  return list;

  /**
   * parseAccountRoot
   * parse balance changes
   * from an account root node
   */

  function parseAccountRoot (node) {

    var account;
    var balance;
    var previous;
    var change;
    var amount;
    var data;
    var fee;

    if (node.FinalFields && node.PreviousFields &&
        node.FinalFields.Balance && node.PreviousFields.Balance) {

      balance  = new BigNumber(node.FinalFields.Balance);
      previous = new BigNumber(node.PreviousFields.Balance);
      account  = node.FinalFields.Account;

    } else if (node.NewFields) {
      balance  = new BigNumber(node.NewFields.Balance);
      previous = new BigNumber(0);
      account  = node.NewFields.Account;

    } else {
      return;
    }

    change = balance.minus(previous);

    if (tx.Account === account) {
      fee    = new BigNumber (tx.Fee).negated();
      amount = change.minus(fee);

      list.push({
        account       : account,
        currency      : 'XRP',
        change        : fee.dividedBy(XRP_ADJUST).toString(),
        final_balance : balance.minus(amount).dividedBy(XRP_ADJUST).toString(),
        time          : tx.executed_time,
        ledger_index  : tx.ledger_index,
        tx_index      : tx.tx_index,
        node_index    : 'fee',
        tx_hash       : tx.hash,
        client        : tx.client,
        type          : 'fee'
      });

    } else {
      amount = change;
    }

    if (!amount.isZero()) {
      data = {
        account       : account,
        currency      : 'XRP',
        change        : amount.dividedBy(XRP_ADJUST).toString(),
        final_balance : balance.dividedBy(XRP_ADJUST).toString(),
        time          : tx.executed_time,
        ledger_index  : tx.ledger_index,
        tx_index      : tx.tx_index,
        node_index    : node.nodeIndex,
        tx_hash       : tx.hash,
        client        : tx.client
      }

      data.type = findType(account, data);
      list.push(data);
    }

  }

  /**
   * parseRippleState
   * parse balances changes
   * from a ripple state node
   */

  function parseRippleState (node) {
    var balance;
    var previous;
    var change;
    var currency;
    var account;
    var issuer;
    var highParty;
    var lowParty;

    // trust line modified
    if (tx.TransactionType === 'TrustSet') {
      return;

    // simple trust line
    } else if (node.NewFields && node.NewFields.Balance.value === '0') {
      return;

    // trustline created with non-zero balance
    } else if (node.NewFields) {
      currency  = node.NewFields.Balance.currency;
      highParty = node.NewFields.HighLimit.issuer;
      lowParty  = node.NewFields.LowLimit.issuer;
      balance   = new BigNumber(node.NewFields.Balance.value);
      change    = new BigNumber(node.NewFields.Balance.value);

    // trustline balance modified
    } else if (node.PreviousFields && node.PreviousFields.Balance) {
      currency  = node.FinalFields.Balance.currency;
      highParty = node.FinalFields.HighLimit.issuer;
      lowParty  = node.FinalFields.LowLimit.issuer;
      previous  = new BigNumber(node.PreviousFields.Balance.value)
      balance   = new BigNumber(node.FinalFields.Balance.value)
      change    = balance.minus(previous);

    // what else?
    } else {
      return;
    }

    if (balance.isNegative() || (previous && previous.isNegative())) {
      account = highParty;
      issuer  = lowParty;
      balance = balance.negated();
      change  = change.negated();

    } else {
      account = lowParty;
      issuer  = highParty;
    }

    var data = {
      account       : account,
      currency      : currency,
      issuer        : issuer,
      change        : change.toString(),
      final_balance : balance.toString(),
      time          : tx.executed_time,
      ledger_index  : tx.ledger_index,
      tx_index      : tx.tx_index,
      node_index    : node.nodeIndex,
      tx_hash       : tx.hash,
      client        : tx.client
    }


    data.type = findType(account, data);
    list.push(data);
  }

  /**
   * findType
   * determine what type of balnace
   * change this is, if possible
   */

  function findType (account, data) {

    //all balance changes for offer creates are exchanges
    if (tx.TransactionType === 'OfferCreate') {
      return 'exchange';

    } else if (tx.TransactionType === 'Payment') {

      //this isn't really a payment, its a conversion/exchange
      if (tx.Account === tx.Destination) {
        return 'exchange';

      // payment currency and destination account
      } else if (account === tx.Destination &&
                 tx.Amount.currency &&
                 tx.Amount.currency === data.currency) {
        return 'payment_destination';

      // payment currency = XRP and destination account
      } else if (account === tx.Destination &&
                 !tx.Amount.currency &&
                 data.currency === 'XRP') {
        return 'payment_destination';

      // source currency and source account
      } else if (account === tx.Account &&
                tx.SendMax &&
                tx.SendMax.currency &&
                tx.SendMax.currency === data.currency) {
        return 'payment_source';

      // source currency = XRP and source account
      } else if (account === tx.Account &&
                tx.SendMax &&
                data.currency === 'XRP') {
        return 'payment_source';

      // source account and destination currency
      } else if (account === tx.Account &&
                 tx.Amount.currency &&
                 tx.Amount.currency === data.currency) {
        return 'payment_source';

      // source account and destination currency
      } else if (account === tx.Account &&
                 !tx.Amount.currency &&
                 data.currency === 'XRP') {
        return 'payment_source';

      // not sender, receiver, or different currency
      } else {
        return 'exchange';
      }
    }

    return null;
  }
};

module.exports = BalanceChanges;
