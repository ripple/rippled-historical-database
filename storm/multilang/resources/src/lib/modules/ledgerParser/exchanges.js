var Amount    = require('ripple-lib').Amount;
var BigNumber = require('bignumber.js');

/**
 * OffersExercised;
 * parse a single transaction to extract 
 * all offers exercised
 */

var OffersExercised = function (tx) {
  var list = [];
  
  if (tx.metaData.TransactionResult !== 'tesSUCCESS') {
    return list;
  }
  
  if (tx.TransactionType !== 'Payment' && tx.TransactionType !== 'OfferCreate') {
    return list;
  }

  tx.metaData.AffectedNodes.forEach(function(affNode, i) {
    var node = affNode.ModifiedNode || affNode.DeletedNode;

    if (!node || node.LedgerEntryType !== 'Offer') {
      return list;
    }
    
    if (!node.PreviousFields || !node.PreviousFields.TakerPays || !node.PreviousFields.TakerGets) {
      return list;
    }

    node.nodeIndex = i;
    list.push(parseOfferExercised(node, tx));    
  });
  
  return list;
  
  /**
   * parseOfferExercised
   * after determining the presence of an
   * excercised offer, extract it into
   * the required form
   */
  
  function parseOfferExercised (node, tx) {


    var counterparty = node.FinalFields.Account;
    var base;
    var counter;
    var exchangeRate;
    var change;

    if ( typeof node.PreviousFields.TakerPays === "object" ) {
      change = Amount.from_json(node.PreviousFields.TakerPays)
        .subtract(node.FinalFields.TakerPays)
        .applyInterest(new Date(tx.executed_time * 1000))
        .to_json().value;
      
      base = {
        currency : node.PreviousFields.TakerPays.currency,
        issuer   : node.PreviousFields.TakerPays.issuer,
        amount   : change
      }
      
    } else {
      change = new BigNumber(node.PreviousFields.TakerPays).minus(node.FinalFields.TakerPays);
      base   = {
        currency : 'XRP',
        amount   : change.dividedBy(1000000.0).toString()
      }
    }

    if ( typeof node.PreviousFields.TakerGets === "object" ) {
      change = Amount.from_json(node.PreviousFields.TakerGets)
        .subtract(node.FinalFields.TakerGets)
        .applyInterest(new Date(tx.executed_time * 1000))
        .to_json().value;
      
      counter = {
        currency : node.PreviousFields.TakerGets.currency,
        issuer   : node.PreviousFields.TakerGets.issuer,
        amount   : change
      }
      
    } else {
      change  = new BigNumber(node.PreviousFields.TakerGets).minus(node.FinalFields.TakerGets);
      counter = {
        currency : 'XRP',
        amount   : change.dividedBy(1000000.0).toString()
      }
    }
    
    exchangeRate = Amount.from_quality(node.FinalFields.BookDirectory, base.currency, base.issuer, {
      base_currency  : counter.currency  
    })
    .applyInterest(new Date(tx.executed_time * 1000))
    .invert()
    .to_json().value;
   
    
    //this case is a patch for the circumstance where the demmurage currency is
    //in takerGets.  The above does not handle this, and probably does not handle
    //the case where both gets and pays are demmuraged.
    if (!exchangeRate) {
      exchangeRate = new BigNumber(counter.amount).dividedBy(base.amount).toString();
    }

  /*  
    console.log('\n');
    console.log(base.currency, counter.currency);
    console.log(new BigNumber(counter.amount).dividedBy(base.amount).toString());
    console.log(exchangeRate);
    console.log('\n');
  */
    
    var offer = {
      base         : base,
      counter      : counter,
      rate         : exchangeRate,
      buyer        : counterparty,
      seller       : tx.Account,
      taker        : tx.Account,
      time         : tx.executed_time,
      tx_index     : tx.tx_index,
      ledger_index : tx.ledger_index,
      node_index   : node.nodeIndex,
      tx_hash      : tx.hash,
      client       : tx.client
    };

/*    
    var exchangeRate = Amount.from_quality(node.FinalFields.BookDirectory).to_json().value;
    var counterparty = node.FinalFields.Account;
    var base;
    var counter;

    if ( typeof node.PreviousFields.TakerPays === "object" ) {
      base = {
        currency : node.PreviousFields.TakerPays.currency,
        issuer   : node.PreviousFields.TakerPays.issuer,
        amount   : node.PreviousFields.TakerPays.value - node.FinalFields.TakerPays.value
      }
    } else {
      base = {
        currency : 'XRP',
        amount   : (node.PreviousFields.TakerPays - node.FinalFields.TakerPays) / 1000000.0 
      }
      
      exchangeRate = exchangeRate / 1000000.0;
    }

    if ( typeof node.PreviousFields.TakerGets === "object" ) {
      counter = {
        currency : node.PreviousFields.TakerGets.currency,
        issuer   : node.PreviousFields.TakerGets.issuer,
        amount   : node.PreviousFields.TakerGets.value - node.FinalFields.TakerGets.value
      }
      
    } else {
      counter = {
        currency : 'XRP',
        amount   : (node.PreviousFields.TakerGets - node.FinalFields.TakerGets) / 1000000.0
      }
      
      exchangeRate = exchangeRate * 1000000.0;
    }
    
    var offer = {
      base         : base,
      counter      : counter,
      rate         : 1 / exchangeRate,
      buyer        : counterparty,
      seller       : tx.Account,
      taker        : tx.Account,
      time         : tx.executed_time,
      tx_index     : tx.tx_index,
      ledger_index : tx.ledger_index,
      node_index   : node.nodeIndex,
      tx_hash      : tx.hash
    };
 */   
    return orderPair(offer);
  }
  
  /**
   * orderPair
   * swap currencies based on
   * lexigraphical order
   */
  
  function orderPair (offer) {
    var c1 = (offer.base.currency + offer.base.issuer).toLowerCase();
    var c2 = (offer.counter.currency + offer.counter.issuer).toLowerCase();
    var swap;
    
    if (c2 < c1) {
      swap          = offer.base;
      offer.base    = offer.counter;
      offer.counter = swap;
      offer.rate    = new BigNumber(offer.rate).pow(-1).toString();
      swap          = offer.buyer;
      offer.buyer   = offer.seller;
      offer.seller  = swap;
    }
    
    return offer;
  }
};

module.exports = OffersExercised;