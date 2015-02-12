var XRP_ADJUST = 1000000.0;

var Payments = function (tx) {
  var payment = { };
  var amount;
  
  if (tx.metaData.TransactionResult !== 'tesSUCCESS') {
    return null;
  }
  
  if (tx.TransactionType !== 'Payment') {
    return null;
  }
  
  //ignore 'convert' payments
  if (tx.Account === tx.Destination) {
    return null;
  }
  
  payment.source      = tx.Account;
  payment.destination = tx.Destination;
  payment.source_balance_changes = [];
  payment.destination_balance_changes = [];
  
  if (tx.DestionationTag) {
    payment.destination_tag = tx.DestionationTag;
  }
  
  if (tx.SourceTag) {
    payment.source_tag = tx.SourceTag;
  }
  
  //destination amount and currency
  if (typeof tx.Amount === 'object') {
    payment.currency = tx.Amount.currency;
    payment.amount   = tx.Amount.value;
    
  } else {
    payment.currency = 'XRP';
    payment.amount   = tx.Amount / XRP_ADJUST;
  }
  
  //delivered amount fields
  amount = tx.metaData.DeliveredAmount || tx.Amount;
  if (typeof amount === 'object') {
    payment.delivered_amount   = amount.value;
    
  } else {
    payment.delivered_amount   = amount / XRP_ADJUST;
  }  
  
  //max amount
  if (typeof tx.SendMax === 'object') {
    payment.max_amount      = tx.SendMax.value;
    payment.source_currency = tx.SendMax.currency;
    
  } else if (tx.SendMax) {
    payment.max_amount = tx.SendMax / XRP_ADJUST;
    payment.source_currency = 'XRP';
  }
  
  
  //get balance changes for the sender and receiver
  tx.metaData.AffectedNodes.forEach(function(affNode, i) {
    var node = affNode.ModifiedNode || affNode.CreatedNode || affNode.DeletedNode;
    var fields;
    var final;
    var previous;
    var amount;
    var highParty;
    var lowParty;
    var sendAmount;
    
    if (!node) {
      return;
    }
    
    fields = node.FinalFields || node.NewFields;
    
    if (!fields) {
      return;
    }
    
    if (node.LedgerEntryType === "AccountRoot") {
    
      final    = fields.Balance;
      previous = node.PreviousFields ? node.PreviousFields.Balance : 0;
      change   = final - previous;
      
      if (fields.Account === tx.Destination) {
        payment.destination_balance_changes.push({
          currency : 'XRP',
          amount   : change / XRP_ADJUST
        });
        
        
      } else if (fields.Account === tx.Account) {
        var fee = parseInt(tx.Fee, 10);
        change += fee;
        if (change) {
          payment.source_balance_changes.push({
            currency : 'XRP',
            amount   : change / XRP_ADJUST
          });
        }

      } else {
        return;
      }
      
    } else if (node.LedgerEntryType === 'RippleState') {

      highParty = fields.HighLimit.issuer;
      lowParty  = fields.LowLimit.issuer;
      previous  = node.PreviousFields ? parseFloat(node.PreviousFields.Balance.value) : 0;
      final     = parseFloat(fields.Balance.value);
      change    = final - previous;
      
      if (final < 0 || previous < 0) {
        issuer  = lowParty;
        account = highParty;
        final   = 0 - final;
        change  = 0 - change;
        
      } else {
        issuer  = highParty;
        account = lowParty;
      }
      
      if (tx.Account === account ||
          tx.Account === issuer) {
        payment.source_balance_changes.push({
          currency : fields.Balance.currency,
          issuer   : issuer,
          amount   : change
        });  
      }

      if (tx.Destination === account ||
          tx.Destination === issuer) {
        payment.destination_balance_changes.push({
          currency : fields.Balance.currency,
          issuer   : issuer,
          amount   : change
        });  
      }
    }   
  });
  
  if (!payment.source_currency && payment.source_balance_changes.length) {
    payment.source_currency = payment.source_balance_changes[0].currency;
  }
  
  payment.fee          = parseInt(tx.Fee, 10) / XRP_ADJUST;
  payment.ledger_index = tx.ledger_index;
  payment.tx_index     = tx.tx_index;
  payment.time         = tx.executed_time;
  payment.tx_hash      = tx.hash;
  
  return payment;
}

module.exports = Payments;