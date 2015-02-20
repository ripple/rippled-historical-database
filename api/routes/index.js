module.exports = function (options) {
  var Routes = { };

  Routes.accountBalances = require('./accountBalances')(options.postgres);
  Routes.accountTx       = require('./accountTx')(options.postgres);
  Routes.getLedger       = require('./getLedger')(options.postgres);
  Routes.getTx           = require('./getTx')(options.postgres);
  Routes.accountTxSeq    = require('./accountTxSeq')(options.postgres);
  Routes.getPayments     = require('./getPayments')(options.hbase);
  Routes.getChanges      = require('./accountBalanceChanges')(options.hbase);
  Routes.getExchanges    = require('./getExchanges')(options.hbase);
  
  return Routes;
};
