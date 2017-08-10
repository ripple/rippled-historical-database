'use strict'

var BigNumber = require('bignumber.js')
var XRP_ADJUST = 1000000.0

/**
 * OffersExercised
 * parse a single transaction to extract
 * all offers exercised
 */

function BalanceChanges(tx) {
  var list = []
  var escrows = {}
  var paychan = {}

  /**
   * findType
   * determine what type of balnace
   * change this is, if possible
   */

  function findType(data) {

    // exchange issuer/intermediary
    if (tx.TransactionType === 'OfferCreate' &&
       Number(data.final_balance) < 0) {
      return 'intermediary'

    // offer creates are all exchanges
    } else if (tx.TransactionType === 'OfferCreate') {
      return 'exchange'

    } else if (tx.TransactionType === 'Payment') {

      // not a real payment issuer on exchange
      if (tx.Account === tx.Destination &&
        Number(data.final_balance) < 0) {
        return 'intermediary'

      // not a real payment just an exchange
      } else if (tx.Account === tx.Destination) {
        return 'exchange'

      // payment currency and destination account
      } else if (data.account === tx.Destination &&
                 tx.Amount.currency &&
                 tx.Amount.currency === data.currency) {
        return 'payment_destination'

      // payment currency = XRP and destination account
      } else if (data.account === tx.Destination &&
                 !tx.Amount.currency &&
                 data.currency === 'XRP') {
        return 'payment_destination'

      // source currency and source account
      } else if (data.account === tx.Account &&
                tx.SendMax &&
                tx.SendMax.currency &&
                tx.SendMax.currency === data.currency) {
        return 'payment_source'

      // source currency = XRP and source account
      } else if (data.account === tx.Account &&
                tx.SendMax &&
                data.currency === 'XRP') {
        return 'payment_source'

      // source account and destination currency
      } else if (data.account === tx.Account &&
                 tx.Amount.currency &&
                 tx.Amount.currency === data.currency) {
        return 'payment_source'

      // source account and destination currency
      } else if (data.account === tx.Account &&
                 !tx.Amount.currency &&
                 data.currency === 'XRP') {
        return 'payment_source'

      // issuer
      } else if (Number(data.final_balance) < 0) {
        return 'intermediary'

      // not sender, receiver, or different currency
      } else {
        return 'exchange'
      }
    }

    return null
  }

  /**
   * parseAccountRoot
   * parse balance changes
   * from an account root node
   */

  function parseAccountRoot(node) {

    var account
    var balance
    var previous
    var change
    var amount
    var data
    var fee

    if (node.FinalFields && node.PreviousFields &&
        node.FinalFields.Balance && node.PreviousFields.Balance) {

      balance = new BigNumber(node.FinalFields.Balance)
      previous = new BigNumber(node.PreviousFields.Balance)
      account = node.FinalFields.Account

    } else if (node.NewFields) {
      balance = new BigNumber(node.NewFields.Balance)
      previous = new BigNumber(0)
      account = node.NewFields.Account

    } else {
      return
    }

    change = balance.minus(previous)

    if (tx.Account === account) {
      fee = new BigNumber(tx.Fee).negated()
      amount = change.minus(fee)

      list.push({
        account: account,
        currency: 'XRP',
        change: fee.dividedBy(XRP_ADJUST).toString(),
        final_balance: balance.minus(amount).dividedBy(XRP_ADJUST).toString(),
        time: tx.executed_time,
        ledger_index: tx.ledger_index,
        tx_index: tx.tx_index,
        node_index: -1,
        tx_hash: tx.hash,
        client: tx.client,
        type: 'fee'
      })

    } else {
      amount = change
    }

    if (!amount.isZero()) {
      data = {
        account: account,
        currency: 'XRP',
        change: amount.dividedBy(XRP_ADJUST).toString(),
        final_balance: balance.dividedBy(XRP_ADJUST).toString(),
        time: tx.executed_time,
        ledger_index: tx.ledger_index,
        tx_index: tx.tx_index,
        node_index: node.nodeIndex,
        tx_hash: tx.hash,
        client: tx.client
      }

      data.type = findType(data)
      list.push(data)
    }

  }

  /**
   * parseRippleState
   * parse balances changes
   * from a ripple state node
   */

  function parseRippleState(node) {
    var balance
    var previous
    var change
    var currency
    var highParty
    var lowParty
    var data

    // only Payments and OfferCreates
    if (tx.TransactionType !== 'Payment' &&
        tx.TransactionType !== 'OfferCreate') {
      return

    // simple trust line
    } else if (node.NewFields && node.NewFields.Balance.value === '0') {
      return

    // trustline created with non-zero balance
    } else if (node.NewFields) {
      currency = node.NewFields.Balance.currency
      highParty = node.NewFields.HighLimit.issuer
      lowParty = node.NewFields.LowLimit.issuer
      balance = new BigNumber(node.NewFields.Balance.value)
      change = new BigNumber(node.NewFields.Balance.value)

    // trustline balance modified
    } else if (node.PreviousFields && node.PreviousFields.Balance) {

      currency = node.FinalFields.Balance.currency
      highParty = node.FinalFields.HighLimit.issuer
      lowParty = node.FinalFields.LowLimit.issuer
      previous = new BigNumber(node.PreviousFields.Balance.value)
      balance = new BigNumber(node.FinalFields.Balance.value)
      change = balance.minus(previous)

    // what else?
    } else {
      return
    }

    data = {
      account: lowParty,
      counterparty: highParty,
      currency: currency,
      change: change.toString(),
      final_balance: balance.toString(),
      time: tx.executed_time,
      ledger_index: tx.ledger_index,
      tx_index: tx.tx_index,
      node_index: node.nodeIndex,
      tx_hash: tx.hash,
      client: tx.client
    }

    data.type = findType(data)
    list.push(data)

    data = {
      account: highParty,
      counterparty: lowParty,
      currency: currency,
      change: change.negated().toString(),
      final_balance: balance.negated().toString(),
      time: tx.executed_time,
      ledger_index: tx.ledger_index,
      tx_index: tx.tx_index,
      node_index: node.nodeIndex,
      tx_hash: tx.hash,
      client: tx.client
    }

    data.type = findType(data)
    list.push(data)
  }

  if (tx.metaData.TransactionResult.indexOf('tec') !== 0 &&
      tx.metaData.TransactionResult !== 'tesSUCCESS') {
    return list
  }

  tx.metaData.AffectedNodes.forEach(function(affNode, i) {
    var node = affNode.ModifiedNode ||
        affNode.CreatedNode ||
        affNode.DeletedNode
    var fields

    if (!node) {
      return
    }

    node.nodeIndex = i
    if (node.LedgerEntryType === 'AccountRoot') {
      parseAccountRoot(node)

    } else if (node.LedgerEntryType === 'RippleState') {
      parseRippleState(node)
    }

    if (node.LedgerEntryType === 'Escrow') {
      fields = node.NewFields || node.FinalFields
      escrows[fields.Account] = fields
      escrows[fields.Destination] = fields
    }

    if (node.LedgerEntryType === 'PayChannel') {
      fields = node.NewFields || node.FinalFields
      paychan[fields.Account] = fields
      paychan[fields.Destination] = fields
    }
  })

  list.forEach(function(d) {
    var amount
    var e

    if (escrows[d.account] && d.type === null) {
      e = escrows[d.account]
      amount = new BigNumber(e.Amount)
        .dividedBy(XRP_ADJUST)
      d.escrow_counterparty = e.Destination

      if (tx.TransactionType === 'EscrowCreate') {
        d.type = 'escrow_create'
        d.escrow_balance_change = amount.toString()

      } else if (tx.TransactionType === 'EscrowCancel') {
        d.type = 'escrow_cancel'
        d.escrow_balance_change = new BigNumber(0)
          .minus(amount).toString()

      } else if (tx.TransactionType === 'EscrowFinish') {
        d.type = 'escrow_finish'
        d.escrow_balance_change = new BigNumber(0)
          .minus(amount).toString()
      }
    }

    if (paychan[d.account] && d.type === null) {
      e = paychan[d.account]

      if (d.account === e.Account) {
        d.type = 'paychannel_fund'
        d.paychannel_counterparty = e.Destination
        d.paychannel_fund_change = new BigNumber(0)
          .minus(d.change).toString()
        d.paychannel_fund_final_balance = new BigNumber(e.Amount || 0)
          .dividedBy(XRP_ADJUST).toString()
        d.paychannel_final_balance = new BigNumber(e.Balance || 0)
          .dividedBy(XRP_ADJUST).toString()

      } else {
        d.type = 'paychannel_payout'
        d.paychannel_counterparty = e.Account
        d.paychannel_balance_change = new BigNumber(0)
          .minus(d.change).toString()
        d.paychannel_fund_final_balance = new BigNumber(e.Amount || 0)
          .dividedBy(XRP_ADJUST).toString()
        d.paychannel_final_balance = new BigNumber(e.Balance || 0)
          .dividedBy(XRP_ADJUST).toString()
      }
    }
  })

  return list
}

module.exports = BalanceChanges
