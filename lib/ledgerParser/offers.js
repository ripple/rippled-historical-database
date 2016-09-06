var BigNumber    = require('bignumber.js');
var parseQuality = require('./quality.js');
var XRP_ADJUST   = 1000000.0;
var EPOCH_OFFSET = 946684800;

var Offers = function (tx) {
  var list = [];
  var affNode;
  var node;
  var fields;
  var type;
  var change_type;
  var pays;
  var gets;
  var offer;
  var value;

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
      type = 'CreatedNode';
    } else if (affNode.ModifiedNode) {
      node = affNode.ModifiedNode;
      type = 'ModifiedNode';
    } else if (affNode.DeletedNode) {
      node = affNode.DeletedNode;
      type = 'DeletedNode';
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
      tx_type         : tx.TransactionType,
      node_type       : type,
      account         : fields.Account,
      offer_sequence  : fields.Sequence,
      expiration      : fields.Expiration,
      book_directory  : fields.BookDirectory,
      tx_hash         : tx.hash,
      executed_time   : tx.executed_time,
      ledger_index    : tx.ledger_index,
      tx_index        : tx.tx_index,
      node_index      : i
    };

    // track old and new offers
    if (tx.OfferSequence && fields.Account === tx.Account) {
      if (type === 'CreatedNode') {
        offer.prev_offer_sequence = tx.OfferSequence;

      } else if (type === 'DeletedNode') {
        offer.next_offer_sequence = tx.Sequence;
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

    try {
      offer.rate = parseQuality(
        fields.BookDirectory,
        offer.taker_pays.currency,
        offer.taker_gets.currency
      ).toString();

    } catch(e) {
      console.log('unable to calculate rate', e);
    }

    //adjust to unix time
    if (offer.expiration) {
      offer.expiration += EPOCH_OFFSET;
    }

    // determine change amounts
    if (node.PreviousFields) {
      if (!node.PreviousFields.TakerPays) {
        offer.pays_change = '0';

      } else if (offer.taker_pays.currency === 'XRP') {
        offer.pays_change = new BigNumber(node.PreviousFields.TakerPays)
          .dividedBy(XRP_ADJUST)
          .minus(offer.taker_pays.value)
          .toString()

      } else {
         offer.pays_change = new BigNumber(node.PreviousFields.TakerPays.value)
          .minus(offer.taker_pays.value)
          .toString()
      }

      if (!node.PreviousFields.TakerGets) {
        offer.gets_change = '0';

      } else if (offer.taker_gets.currency === 'XRP') {
        offer.gets_change = new BigNumber(node.PreviousFields.TakerGets)
          .dividedBy(XRP_ADJUST)
          .minus(offer.taker_gets.value)
          .toString()

      } else {
         offer.gets_change = new BigNumber(node.PreviousFields.TakerGets.value)
          .minus(offer.taker_gets.value)
          .toString()
      }
    } else {
      offer.pays_change = '0';
      offer.gets_change = '0';
    }

    // created node is only a new offer
    if (type === 'CreatedNode') {
      offer.change_type = 'create';

    // all modified nodes are partial fill
    } else if (type === 'ModifiedNode') {
      offer.change_type = 'partial_fill';

    // all offer cancel is cancel
    } else if (tx.TransactionType === 'OfferCancel') {
      offer.change_type = 'cancel';

    // replace
    } else if (tx.TransactionType === 'OfferCreate' &&
               type === 'DeletedNode' &&
               fields.Account === tx.Account &&
               fields.Sequence === tx.OfferSequence) {
      offer.change_type = 'replace';

    // no amount means filled offer
    } else if (offer.taker_pays.value === '0') {
      offer.change_type = 'fill';

    } else if (!node.PreviousFields) {
      offer.change_type = 'unfunded_cancel';

    } else if (offer.pays_change !== '0' ||
              offer.gets_change !== '0') {
      offer.change_type = 'unfunded_partial_fill';
    }

    list.push(offer);
  }

  return list;
}

module.exports = Offers;
