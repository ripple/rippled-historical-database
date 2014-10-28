var log    = require('../log')('offers_exercised');
var Amount = require('ripple-lib').Amount;
var order  = ['XAU','XAG','BTC','XRP','EUR','GBP','AUD','NZD','USD','CAD','CHF','JPY','CNY'];

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
      counterparty : counterparty,
      account      : tx.Account,
      nodeIndex    : node.nodeIndex
    };
    
    return orderPair(offer);
  }
  
  /**
   * orderPair
   * format the results according to the 
   * the accepted hierachy of currencies.
   * for same IOU currencies and ones not in
   * the list, sort alphabetically by currency + issuer
   */
  
  function orderPair (offer) {
    var a = order.indexOf(offer.base.currency);
    var b = order.indexOf(offer.counter.currency);
    var swap;
    
    log.debug(a, b);
    
    if ((a === -1 && b === -1) || a === b) {
      var c1 = (offer.base.currency + offer.base.issuer).toLowerCase();
      var c2 = (offer.counter.currency + offer.counter.issuer).toLowerCase();
      return  c1 < c2 ?  offer : swapPairs(offer); 
      
    } else if (a === -1) {
      return swapPairs(offer);
      
    } else if (b !== -1 && b < a) {
      return swapPairs(offer);
    
    } else {
      return offer;
    }
    
    /**
     * swapPairs
     * switch base, counter, account, counterparty
     * and invert the exchange rate
     */
    
    function swapPairs (offer) {
      var swap;
      log.debug("swapped:", offer.base.currency, offer.counter.currency);
      
      swap          = offer.base;
      offer.base    = offer.counter;
      offer.counter = swap;
      offer.rate    = 1 / offer.rate;
      swap          = offer.account;
      offer.account = offer.counterparty;
      offer.counterparty = swap;
      
      return offer;
    }
  }
};

module.exports = OffersExercised;