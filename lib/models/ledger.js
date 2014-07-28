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
  underscored: true,

  classMethods: {
    createFromJSON: function(json, fn) {
      Ledger.create({
          id: json.ledger_index,
          hash: json.hash,
          sequence: json.seqHash,
          prev_hash: json.parent_hash,
          total_coins: json.total_coins,
          closing_time: json.close_time,
          //prev_closing_time: json.,
          close_time_resolution: json.close_time_resolution,
          //close_flags: json.,
          account_set_hash: json.account_hash
      }).complete(fn);
    },

    setOfSavedLedgers: function(fn) {
      Ledger.findAll({
        attributes: ['id']
      }).complete(fn);
    }
  }
});

module.exports = Ledger;
