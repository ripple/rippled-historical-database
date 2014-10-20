var AccountsCreated = function (tx) {
  var list = [];
  
  if ( tx.metaData.TransactionResult !== "tesSUCCESS" ) {
    return list;
  }

  tx.metaData.AffectedNodes.forEach( function(affNode) {
    if (affNode.CreatedNode && affNode.CreatedNode.LedgerEntryType === "AccountRoot") {
      list.push({
        account : affNode.CreatedNode.NewFields.Account, 
        parent  : tx.Account
      });
    }
  });
  
  return list;
};

module.exports = AccountsCreated;