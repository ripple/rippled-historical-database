var Routes = { };

module.exports = function (options) {
  Routes.accountBalances = require('./accountBalances')(options.postgres);
  Routes.accountTx       = require('./accountTx')(options.postgres);
  Routes.getLedger       = require('./getLedger')(options.postgres);
  Routes.getTx           = require('./getTx')(options.postgres);
  Routes.accountTxSeq    = require('./accountTxSeq')(options.postgres);
  
  return Routes;
};
