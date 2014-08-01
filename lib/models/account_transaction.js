var db = require('../sequelize.js');
var Sequelize = require('sequelize');

var AccountTransaction = db.define('account_transactions', {
  transaction_id: {
    type: Sequelize.BIGINT,
    primaryKey: true
  },
  account_id:           Sequelize.BIGINT,
  ledger_sequence:      Sequelize.BIGINT,
  transaction_sequence: Sequelize.BIGINT
}, {
  timestamps: false,
  underscored: true
});

module.exports = AccountTransaction;
