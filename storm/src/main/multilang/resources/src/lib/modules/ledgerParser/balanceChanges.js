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
  
  tx.metaData.AffectedNodes.forEach( function(affNode, i) {
    var node = affNode.ModifiedNode || affNode.CreatedNode || affNode.DeletedNode;

    if (!node) {
      return;
    }
    
    node.nodeIndex = i;
    if (node.LedgerEntryType === "AccountRoot" ) {
      parseAccountRoot(node);

    } else if (node.LedgerEntryType === "RippleState") {
      parseRippleState(node, tx.Account, tx.Destination);
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
    var amount;
    var data;
    var fee;
    
    if (fields) {
      balance  = fields.Balance,
      previous = node.PreviousFields ? node.PreviousFields.Balance : 0,
      change   = balance - previous;
      
      if (tx.Account === fields.Account) {
        fee    = parseInt(tx.Fee, 10);
        amount = change + fee;
        
        list.push({
          account       : fields.Account,
          currency      : 'XRP',
          change        : 0 - fee / XRP_ADJUST,
          final_balance : (balance - amount) / XRP_ADJUST,
          time          : tx.executed_time,
          ledger_index  : tx.ledger_index,
          tx_index      : tx.tx_index,
          node_index    : 'fee',
          tx_hash       : tx.hash,
          type          : 'network fee'
        });
        
      } else {
        amount = change;
      }
      
      if (amount) {
        data = {
          account       : fields.Account,
          currency      : 'XRP',
          change        : amount / XRP_ADJUST,
          final_balance : balance / XRP_ADJUST,
          time          : tx.executed_time,
          ledger_index  : tx.ledger_index,
          tx_index      : tx.tx_index,
          node_index    : node.nodeIndex,
          tx_hash       : tx.hash
        }
        
        data.type = findType(fields.Account, data);
        list.push(data);
      }
    } 
  }
  
  /**
   * parseRippleState
   * parse balances changes
   * from a ripple state node
   */
  
  function parseRippleState (node, initiator, destination) {
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
    
    if (balance < 0 || previous < 0) {    
      account = highParty;
      issuer  = lowParty;  
      balance = 0 - balance;
      change  = 0 - change;
    
    //sending to issuer
    } else if (lowParty === initiator) {
      account = lowParty;
      issuer  = highParty;      
      
    } else {
      account = lowParty;
      issuer  = highParty;
    }
    
    var data = {
      account       : account,
      currency      : currency,
      issuer        : issuer,
      change        : change,
      fee           : 0,
      final_balance : balance,
      time          : tx.executed_time,
      ledger_index  : tx.ledger_index,
      tx_index      : tx.tx_index,      
      node_index    : node.nodeIndex,
      tx_hash       : tx.hash
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
    if (tx.TransactionType === 'OfferCreate') {
      return 'exchange';
    } else if (tx.TransactionType === 'Payment') {
      if (tx.Account === tx.Destination) {
        return 'exchange';
      } else if (account === tx.Destination) {
        var currency = typeof tx.Amount === 'object' ? tx.Amount.currency : 'XRP';
        var issuer   = currency !== 'XRP' ? tx.Amount.issuer : undefined;                
        
        if (currency === data.currency && issuer === data.issuer) {
          return 'payment';  
        } else {
          return 'exchange';
        }
        
      } else if (account === tx.Account) {
        //????
        return 'payment';
      } else {
        return 'exchange';
      }
    } 
    
    return null;
  }
};

module.exports = BalanceChanges;