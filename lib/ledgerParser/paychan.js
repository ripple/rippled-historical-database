'use strict'

var BigNumber = require('bignumber.js')
var smoment = require('../smoment')
var XRP_ADJUST = 1000000.0
var EPOCH_OFFSET = 946684800

function getPaychannelNode(tx) {
  var node
  for (var i = 0; i < tx.metaData.AffectedNodes.length; i++) {
    node = tx.metaData.AffectedNodes[i].CreatedNode ||
      tx.metaData.AffectedNodes[i].ModifiedNode ||
      tx.metaData.AffectedNodes[i].DeletedNode;

    if (node.LedgerEntryType === 'PayChannel') {
      node.fields = node.NewFields || node.FinalFields
      return node
    }
  }

  return {
    fields: {}
  }
}

function Paychan(tx) {
  var paychan = {}
  var node

  if (tx.metaData.TransactionResult !== 'tesSUCCESS') {
    return undefined
  }

  if ([
    'PaymentChannelCreate',
    'PaymentChannelFund',
    'PaymentChannelClaim'
  ].indexOf(tx.TransactionType) === -1) {
    return undefined
  }

  node = getPaychannelNode(tx)
  paychan.fee = new BigNumber(tx.Fee).dividedBy(XRP_ADJUST).toString()
  paychan.flags = tx.Flags
  paychan.ledger_index = tx.ledger_index
  paychan.tx_index = tx.tx_index
  paychan.time = tx.executed_time
  paychan.tx_hash = tx.hash
  paychan.tx_type = tx.TransactionType
  paychan.client = tx.client
  paychan.channel = tx.Channel
  paychan.signature = tx.Signature
  paychan.pubkey = tx.PublicKey
  paychan.settle = tx.SettleDelay
  paychan.account = tx.Account
  paychan.source = node.fields.Account
  paychan.destination = node.fields.Destination
  paychan.destination_tag = node.fields.DestinationTag
  paychan.source_tag = node.fields.SourceTag

  paychan.amount = node.fields.Amount ? new BigNumber(node.fields.Amount)
    .dividedBy(XRP_ADJUST).toString() : undefined
  paychan.balance = node.fields.Balance ? new BigNumber(node.fields.Balance)
    .dividedBy(XRP_ADJUST).toString() : undefined

  if (tx.CancelAfter) {
    paychan.cancel_after = tx.CancelAfter + EPOCH_OFFSET
    paychan.cancel_after = smoment(paychan.cancel_after).format()
  }

  if (tx.Expiration) {
    paychan.expiration = tx.Expiration + EPOCH_OFFSET
    paychan.expiration = smoment(paychan.expiration).format()
  }

  return paychan
}

module.exports = Paychan
