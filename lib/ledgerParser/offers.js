var BigNumber    = require('bignumber.js');
var XRP_ADJUST   = 1000000.0;
var EPOCH_OFFSET = 946684800;

var Offers = function (tx) {
  var list = [];
  var affNode;
  var node;
  var fields;
  var type;
  var pays;
  var gets;
  var offer;

  if (tx.metaData.TransactionResult !== 'tesSUCCESS') {
    return list;
  }

  if (['Payment','OfferCancel','OfferCreate'].indexOf(tx.TransactionType) === -1) {
    return list;
  }

  for (var i=0; i<tx.metaData.AffectedNodes.length; i++) {
    affNode = tx.metaData.AffectedNodes[i];

    if (affNode.CreatedNode) {
      node = affNode.CreatedNode;
      type = 'create';
    } else if (affNode.ModifiedNode) {
      node = affNode.ModifiedNode;
      type = 'modify';
    } else if (affNode.DeletedNode) {
      node = affNode.DeletedNode;
      type = 'cancel';
    } else {
      continue;
    }

    if (node.LedgerEntryType !== 'Offer') {
      continue;
    }

    fields = node.NewFields || node.FinalFields;

    //this shouldnt happen, (i think)
    if (!fields) continue;

    offer = {
      type          : type,
      account       : fields.Account,
      sequence      : fields.Sequence,
      expiration    : fields.Expiration,
      tx_hash       : tx.hash,
      executed_time : tx.executed_time,
      ledger_index  : tx.ledger_index,
      tx_index      : tx.tx_index,
      node_index    : i
    };

    // track old and new offers
    if (tx.OfferSequence) {
      if (fields.Account === tx.Account && type === 'create') {
        offer.old_offer = tx.OfferSequence;
      } else if (fields.Account && type === 'cancel') {
        offer.new_offer = tx.Sequence;
      }
    }

    if (typeof fields.TakerPays === 'object') {
      offer.taker_pays = fields.TakerPays;
    } else {
      offer.taker_pays = {
        currency: 'XRP',
        value: new BigNumber(fields.TakerPays).dividedBy(XRP_ADJUST).toString()
      };
    }

    if (typeof fields.TakerGets === 'object') {
      offer.taker_gets = fields.TakerGets;
    } else {
      offer.taker_gets = {
        currency: 'XRP',
        value: new BigNumber(fields.TakerGets).dividedBy(XRP_ADJUST).toString()
      };
    }

    //adjust to unix time
    if (offer.expiration) {
      offer.expiration += EPOCH_OFFSET;
    }

    list.push(offer);
  }

  return list;
}

module.exports = Offers;
