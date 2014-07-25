var db = require('../sequelize.js');
var Sequelize = require('sequelize');

var Transaction = db.define('transactions', {
  hash:            Sequelize.BLOB,
  type:            Sequelize.ENUM('Payment', 'OfferCreate', 'OfferCancel',
                                     'AccountSet', 'SetRegularKey',
                                     'TrustSet'),
  from_account:    Sequelize.BIGINT,
  from_sequence:   Sequelize.BIGINT,
  ledger_sequence: Sequelize.BIGINT,
  status:          Sequelize.STRING(1),
  raw:             Sequelize.BLOB,
  meta:            Sequelize.BLOB
}, {
  timestamps: true,
  underscored: true
});

module.exports = Transaction;
