var db = require('../sequelize.js');
var Sequelize = require('sequelize');

var AccountTransaction = db.define('account_transactions', {
  transaction_id:       Sequelize.BIGINT,
  account_id:           Sequelize.BIGINT,
  ledger_sequence:      Sequelize.BIGINT,
  transaction_sequence: Sequelize.BIGINT
}, {
  timestamps: true,
  underscored: true
});

module.exports = AccountTransaction;
