'use strict';

module.exports = function(options) {
  var Routes = { };

  Routes.accountBalances = require('./accountBalances')(options.postgres);
  Routes.accountTx = require('./accountTx')(options.postgres);
  Routes.getLedger = require('./getLedger')(options.hbase);
  Routes.getTx = require('./getTx')(options.postgres);
  Routes.accountTxSeq = require('./accountTxSeq')(options.postgres);
  Routes.accountExchanges = require('./accountExchanges')(options.hbase);
  Routes.accountPayments = require('./accountPayments')(options.hbase);
  Routes.getChanges = require('./accountBalanceChanges')(options.hbase);
  Routes.getExchanges = require('./getExchanges')(options.hbase);
  Routes.getLastValidated = require('./getLastValidated')(options.postgres);
  Routes.accountReports = require('./accountReports')(options.hbase);
  Routes.reports = require('./reports')(options.hbase);
  Routes.stats = require('./stats')(options.hbase);
  Routes.accounts = require('./accounts')(options.hbase);
  Routes.getAccount = require('./getAccount')(options.hbase);

  return Routes;
};
