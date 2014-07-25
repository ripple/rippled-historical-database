var db = require('../sequelize.js');
var Sequelize = require('sequelize');

var LedgerTransaction = db.define('ledger_transactions', {
  transaction_id:        Sequelize.BIGINT,
  ledger_id:             Sequelize.BIGINT,
  transaction_sequence:  Sequelize.BIGINT
}, {
  timestamps: false,
  underscored: true
});

module.exports = LedgerTransaction;
