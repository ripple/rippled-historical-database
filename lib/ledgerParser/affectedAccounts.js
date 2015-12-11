var getAccounts = require('ripple-lib-transactionparser')
  .getAffectedAccounts;

module.exports = function (tx) {
  var accounts = [];
  var list     = [];

  accounts = getAccounts(tx.metaData);

  accounts.forEach(function(account) {
    if (account[0] !== 'r') return;

    list.push({
      account      : account,
      tx_result    : tx.tx_result,
      tx_type      : tx.TransactionType,
      time         : tx.executed_time,
      ledger_index : tx.ledger_index,
      tx_index     : tx.tx_index,
      tx_hash      : tx.hash,
      client       : tx.client
    });
  });

  return list;
};
