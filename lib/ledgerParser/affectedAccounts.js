var utils = require('../utils');
var Base  = require('ripple-lib').Base;

module.exports = function (tx) {
  var accounts = { };
  var list     = [];
  
  addAffectedAccount('initiator', tx.Account, tx);
  
  if (tx.metaData.TransactionResult === 'tesSUCCESS') {
    switch(tx.TransactionType) {  
      case 'Payment':
        addAffectedAccount('source', tx.Account, tx);
        addAffectedAccount('destination', tx.Destination, tx);
        break;
      case 'TrustSet':
        addAffectedAccount('trust', tx.Account, tx);
        addAffectedAccount('trustee', tx.Destination, tx);
        break;        
    }
  }
  
  tx.metaData.AffectedNodes.forEach( function( affNode ) {
    var node = affNode.CreatedNode || affNode.ModifiedNode || affNode.DeletedNode;
    parseNode(node.LedgerEntryType, node.NewFields || node.FinalFields, tx);
  }); 
  
  for(key in accounts) {
    list.push({
      account      : key,
      roles        : accounts[key],
      tx_result    : tx.tx_result,
      tx_type      : tx.TransactionType,
      time         : tx.executed_time,
      ledger_index : tx.ledger_index,
      tx_index     : tx.tx_index,
      tx_hash      : tx.hash
    });
  }
  
  return list;
  
  function parseNode (nodeType, fields, tx) {  
    if (!fields) {
      return;
    }
    
    for (var key in fields) {
      if (isRippleAddress(fields[key])) {
        
        addAffectedAccount(null, fields[key], tx);  
      
        
      } else if (key === 'HighLimit' ||
                 key === 'LowLimit'  ||
                 key === 'TakerPays' ||
                 key === 'TakerGets') {
        
        if (isRippleAddress(fields[key].issuer)) {
          var role = nodeType === 'RippleState' ? null : 'issuer';
          addAffectedAccount(role, fields[key].issuer, tx);  
        }
      }
    }
  }
  
  function addAffectedAccount (role, account, tx) {
    if (!accounts[account]) {
      accounts[account] = role ? [role] : [];
    
    } else if (role && accounts[account].indexOf(role) === -1) {
      accounts[account].push(role);
    }
  }
  
  function isRippleAddress (data) {
    return typeof data === 'string' &&
    data.charAt(0)     === "r"      &&
    Base.decode_check(Base.VER_ACCOUNT_ID, data) ? true : false;  
  }  
};
