var db = require('../sequelize.js');
var Sequelize = require('sequelize');

var Ledger = db.define('ledgers', {
  hash:                  Sequelize.BLOB,
  sequence:              Sequelize.BIGINT,
  prev_hash:             Sequelize.BLOB,
  total_coins:           Sequelize.BIGINT,
  closing_time:          Sequelize.BIGINT,
  prev_closing_time:     Sequelize.BIGINT,
  close_time_resolution: Sequelize.BIGINT,
  close_flags:           Sequelize.BIGINT,
  account_set_hash:      Sequelize.BLOB,
  transaction_set_hash:  Sequelize.BLOB
}, {
  timestamps: false,
  underscored: true
});

module.exports = Ledger;
