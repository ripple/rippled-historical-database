'use strict';

module.exports = function(db) {
  var Routes = { };

  Routes.accountBalances = require('./accountBalances')(db);
  Routes.accountTx = require('./accountTx')(db);
  Routes.getLedger = require('./getLedger')(db);
  Routes.getTx = require('./getTx')(db);
  Routes.accountTxSeq = require('./accountTxSeq')(db);
  Routes.getLastValidated = require('./getLastValidated')(db);

  return Routes;
};
