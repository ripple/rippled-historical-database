var log    = require('../log')('offers_exercised');
var Amount = require('ripple-lib').Amount;
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
  
  tx.metaData.AffectedNodes.forEach( function( affNode ) {
    var node = affNode.ModifiedNode || affNode.CreatedNode || affNode.DeletedNode;

    if (!node) {
      console.log('here');
      return;
    }
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

    var fields = node.FinalFields || node.NewFields;
    var balance;
    var previous;
    var change;
    var fee;
    var type;
    
    if (fields) {
      balance  = fields.Balance,
      previous = node.PreviousFields ? node.PreviousFields.Balance : 0,
      change   = balance - previous;
      
      if (tx.Account === fields.Account) {
        fee     = parseInt(tx.Fee, 10);
        change += fee; 
        
        list.push({
          account  : tx.Account,
          currency : 'XRP',
          change   : 0 - (fee / XRP_ADJUST),
          balance  : (balance - change) / XRP_ADJUST,
          type     : 'fee'
        });
      }

      if (change) {             
        list.push({
          account  : fields.Account,
          currency : 'XRP',
          change   : change / XRP_ADJUST,
          balance  : balance / XRP_ADJUST,    
          type     : findType(fields.Account)
        });
      }
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
    
    if ( node.NewFields ) {

      if ( parseFloat( node.NewFields.Balance.value ) === 0 ) {
        return;
      }

      // trustline created with non-negative balance
      currency  = node.NewFields.Balance.currency;
      highParty = node.NewFields.HighLimit.issuer;
      lowParty  = node.NewFields.LowLimit.issuer;
      previous  = 0;
      balance   = parseFloat(node.NewFields.Balance.value);
      change    = balance - previous;

    } else if (node.PreviousFields && node.PreviousFields.Balance) {

      // trustline balance modified
      currency  = node.FinalFields.Balance.currency;
      highParty = node.FinalFields.HighLimit.issuer;
      lowParty  = node.FinalFields.LowLimit.issuer;
      previous  = parseFloat(node.PreviousFields.Balance.value);
      balance   = parseFloat(node.FinalFields.Balance.value);
      change    = balance - previous;

    } else {
      return;
    }
    
    if (balance < 0) {
      account = highParty;
      issuer  = lowParty;
      balance = 0 - balance;
      change  = 0 - change;
      
    } else {
      account = lowParty;
      issuer  = highParty;
    }
    
    list.push({
      account  : account,
      currency : currency,
      issuer   : issuer,
      change   : change,
      balance  : balance,
      type     : findType(account)
    });
  }
  
  /**
   * findType
   * determine what type of balnace
   * change this is, if possible
   */
  
  function findType (account) {
    if (tx.TransactionType === 'OfferCreate') {
      return 'exchange';
    } else if (tx.TransactionType === 'Payment') {
      if (tx.Account === tx.Destination) {
        return 'exchange';
      } else if (account === tx.Account || account === tx.Destination) {
        return 'payment';
      } else {
        return 'exchange';
      }
    } 
    
    return null;
  }
};

module.exports = BalanceChanges;