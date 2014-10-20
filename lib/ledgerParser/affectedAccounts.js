var utils = require('./utils');
var Base  = require('ripple-lib').Base;

module.exports = function (transaction) {
  var accounts = { };
  var list     = [];
  
  switch(transaction.TransactionType) {
    
    case 'Payment':
      addAffectedAccount('send', transaction.Account, transaction);
      addAffectedAccount('receive', transaction.Destination, transaction);
      break;
    case 'TrustSet':
      addAffectedAccount('trust', transaction.Account, transaction);
      addAffectedAccount('trustee', transaction.Destination, transaction);
      break; 
    case 'OfferCreate':
      addAffectedAccount('createOffer', transaction.Account, transaction);
      break;       
    case 'OfferCancel':
      addAffectedAccount('cancelOffer', transaction.Account, transaction);
      break;       
  }
  
  transaction.metaData.AffectedNodes.forEach( function( affNode ) {
    var node = affNode.CreatedNode || affNode.ModifiedNode || affNode.DeletedNode;
    
    if (node.FinalFields){
      parseNode(node.LedgerEntryType, 'final', node.FinalFields, transaction);
    }

    else if (node.NewFields){
      parseNode(node.LedgerEntryType, 'new', node.NewFields, transaction);
    }
  }); 
  
  for(key in accounts) {
    list.push({
      account : key,
      roles   : accounts[key]
    });
  }
  
  return list;
  
  function parseNode (nodeType, type, fields, transaction) {
    for (var key in fields) {
      if (isRippleAddress(fields[key])) {
        addAffectedAccount(null, fields[key], transaction);  
      
        
      } else if (key === 'HighLimit' ||
                 key === 'LowLimit'  ||
                 key === 'TakerPays' ||
                 key === 'TakerGets') {
        
        if (isRippleAddress(fields[key].issuer)) {
          var role = nodeType === 'RippleState' ? null : 'issuer';
          addAffectedAccount(role, fields[key].issuer, transaction);  
        }
      }
    }
  }
  
  function addAffectedAccount (role, account, transaction) {
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
