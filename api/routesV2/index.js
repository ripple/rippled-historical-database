'use strict';

module.exports = function(db) {
  var Routes = { };

  Routes.getLedger = require('./getLedger')(db);
  Routes.getTransactions = require('./getTransactions')(db);
  Routes.accountTransactions = require('./accountTransactions')(db);
  Routes.accountExchanges = require('./accountExchanges')(db);
  Routes.accountPayments = require('./accountPayments')(db);
  Routes.accountBalances = require('./accountBalances')(db);
  Routes.getChanges = require('./accountBalanceChanges')(db);
  Routes.getExchanges = require('./getExchanges')(db);
  Routes.accountReports = require('./accountReports')(db);
  Routes.reports = require('./reports')(db);
  Routes.stats = require('./stats')(db);
  Routes.accounts = require('./accounts')(db);
  Routes.getAccount = require('./getAccount')(db);
  Routes.getLastValidated = require('./getLastValidated')(db);
  return Routes;
};
