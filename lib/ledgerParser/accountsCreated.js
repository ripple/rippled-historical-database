var XRP_ADJUST = 1000000.0;

var AccountsCreated = function (tx) {
  var list = [];
  
  if ( tx.metaData.TransactionResult !== "tesSUCCESS" ) {
    return list;
  }

  tx.metaData.AffectedNodes.forEach( function(affNode) {
    if (affNode.CreatedNode && affNode.CreatedNode.LedgerEntryType === "AccountRoot") {
      list.push({
        account : affNode.CreatedNode.NewFields.Account, 
        parent  : tx.Account,
        balance : affNode.CreatedNode.NewFields.Balance / XRP_ADJUST,
        time    : tx.executed_time,
        ledger_index : tx.ledger_index,
        tx_index : tx.tx_index,
        tx_hash  : tx.hash,
        client   : tx.client
      });
    }
  });
  
  return list;
};

module.exports = AccountsCreated;