var BigNumber = require('bignumber.js')
var smoment = require('../smoment')
var XRP_ADJUST = 1000000.0
var EPOCH_OFFSET = 946684800

function getEscrowNode(tx) {
  var node
  for (var i = 0; i < tx.metaData.AffectedNodes.length; i++) {
    node = tx.metaData.AffectedNodes[i]
    if (node.DeletedNode &&
        node.DeletedNode.LedgerEntryType === 'Escrow') {
      return node.DeletedNode
    }
  }

  return {
    FinalFields : {}
  }
}

function Escrow(tx) {
  var escrow = {}
  var node

  if (tx.metaData.TransactionResult !== 'tesSUCCESS') {
    return
  }

  if ([
      'EscrowCreate',
      'EscrowCancel',
      'EscrowFinish'
      ].indexOf(tx.TransactionType) === -1) {
    return
  }

  node = getEscrowNode(tx)
  escrow.fee = new BigNumber(tx.Fee).dividedBy(XRP_ADJUST).toString()
  escrow.flags = tx.Flags
  escrow.ledger_index = tx.ledger_index
  escrow.tx_index = tx.tx_index
  escrow.time = tx.executed_time
  escrow.tx_hash = tx.hash
  escrow.tx_type = tx.TransactionType
  escrow.client = tx.client
  escrow.amount = new BigNumber(tx.Amount || node.FinalFields.Amount)
    .dividedBy(XRP_ADJUST).toString()
  escrow.account = tx.Account
  escrow.owner = tx.Account || tx.Owner
  escrow.destination = tx.Destination || node.FinalFields.Destination
  escrow.destination_tag = tx.DestinationTag || node.FinalFields.DestinationTag
  escrow.source_tag = tx.SourceTag || node.FinalFields.SourceTag
  escrow.create_tx_seq = tx.Sequence || tx.OfferSequence
  escrow.create_tx = node.FinalFields.PreviousTxnID || tx.hash
  escrow.condition = tx.Condition
  escrow.fulfillment = tx.Fulfillment

  if (tx.CancelAfter) {
    escrow.cancel_after = tx.CancelAfter + EPOCH_OFFSET
    escrow.cancel_after = smoment(escrow.cancel_after).format()
  }

  if (tx.FinishAfter) {
    escrow.finish_after = tx.FinishAfter + EPOCH_OFFSET
    escrow.finish_after = smoment(escrow.finish_after).format()
  }

  return escrow
}

module.exports = Escrow
