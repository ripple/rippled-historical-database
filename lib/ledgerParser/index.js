'use strict'

var utils = require('../utils')

var EPOCH_OFFSET = 946684800
var Parser = { }

Parser.affectedAccounts = require('./affectedAccounts')
Parser.exchanges = require('./exchanges')
Parser.offers = require('./offers')
Parser.balanceChanges = require('./balanceChanges')
Parser.accountsCreated = require('./accountsCreated')
Parser.memos = require('./memos')
Parser.payment = require('./payment')
Parser.fromClient = require('./fromClient')
Parser.summarizeFees = require('./fees')
Parser.escrow = require('./escrow')

Parser.parseLedger = function(ledger) {
  var data = {
    ledger: null,
    transactions: [],
    affectedAccounts: [],
    accountsCreated: [],
    exchanges: [],
    offers: [],
    balanceChanges: [],
    payments: [],
    escrows: [],
    memos: []
  }

  var transactions = ledger.transactions

  // note: this will only work until 2030
  if (ledger.close_time < EPOCH_OFFSET) {
    ledger.close_time = ledger.close_time + EPOCH_OFFSET
  }

  data.feeSummary = Parser.summarizeFees(ledger)
  ledger.transactions = []

  transactions.forEach(function(transaction) {
    ledger.transactions.push(transaction.hash)
    var meta = transaction.metaData
    var payment
    var escrow

    delete transaction.metaData

    try {
      transaction.raw = utils.toHex(transaction)
      transaction.meta = utils.toHex(meta)

    } catch (e) {
      console.log(e, transaction.ledger_index, transaction.hash)
      return
    }

    transaction.metaData = meta
    transaction.ledger_hash = ledger.ledger_hash
    transaction.ledger_index = ledger.ledger_index
    transaction.executed_time = ledger.close_time
    transaction.tx_index = transaction.metaData.TransactionIndex
    transaction.tx_result = transaction.metaData.TransactionResult

    // set 'client' string, if its present in a memo
    transaction.client = Parser.fromClient(transaction)

    data.transactions.push(transaction)

    data.exchanges = data.exchanges.concat(Parser.exchanges(transaction))
    data.offers = data.offers.concat(Parser.offers(transaction))
    data.balanceChanges =
      data.balanceChanges.concat(Parser.balanceChanges(transaction))
    data.accountsCreated =
      data.accountsCreated.concat(Parser.accountsCreated(transaction))
    data.affectedAccounts =
      data.affectedAccounts.concat(Parser.affectedAccounts(transaction))
    data.memos = data.memos.concat(Parser.memos(transaction))

    // parse payment
    payment = Parser.payment(transaction)
    if (payment) {
      data.payments.push(payment)
    }

    // parse escrow
    escrow = Parser.escrow(transaction)
    if (escrow) {
      data.escrows.push(escrow)
    }
  })

  data.ledger = ledger
  return data
}

/**
 * parseTransaction
 * Parse a single transaction
 */

Parser.parseTransaction = function(tx) {
  var data = { }
  var payment
  var escrow

  data.exchanges = Parser.exchanges(tx)
  data.offers = Parser.offers(tx)
  data.balanceChanges = Parser.balanceChanges(tx)
  data.accountsCreated = Parser.accountsCreated(tx)
  data.affectedAccounts = Parser.affectedAccounts(tx)
  data.memos = Parser.memos(tx)
  data.payments = []
  data.escrows = []
  payment = Parser.payment(tx)
  escrow = Parser.escrow(tx)

  if (payment) {
    data.payments.push(payment)
  }

  if (escrow) {
    data.escrows.push(escrow)
  }

  return data
}

module.exports = Parser
