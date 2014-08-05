var db = require('../sequelize.js').db;
var Sequelize = require('sequelize');

var Ledger = db.define('ledgers', {
  id:                    Sequelize.BIGINT,
  ledger_hash:           Sequelize.BLOB,
  parent_hash:           Sequelize.BLOB,
  total_coins:           Sequelize.BIGINT,
  close_time:            Sequelize.BIGINT,
  close_time_resolution: Sequelize.BIGINT,
  account_hash:          Sequelize.BLOB,
  transaction_hash:      Sequelize.BLOB,
  accepted:              Sequelize.BOOLEAN,
  closed:                Sequelize.BOOLEAN,
  close_time_estimated:  Sequelize.BOOLEAN,
  close_time_human:      Sequelize.DATE

  //parent_close_time:     Sequelize.BIGINT,
  //close_flags:           Sequelize.BIGINT,
  //state_hash:            Sequelize.BLOB,
}, {
  timestamps: false,
  underscored: true,

  classMethods: {
    createFromJSON: function(json, t, fn) {
      Ledger.create({
        id: json.ledger_index,
        ledger_hash: json.ledger_hash,
        parent_hash: json.parent_hash,
        total_coins: json.total_coins,
        close_time: json.close_time,
        close_time_resolution: json.close_time_resolution,
        account_hash: json.account_hash,
        transaction_hash: json.transaction_hash,
        accepted: json.accepted,
        closed: json.closed,
        close_time_estimated: json.close_time_estimated,
        close_time_human: json.close_time_human

        //prev_closing_time: json., // Not in the json response
        //close_flags: json., // Not in the json response
        //state_hash: json. // Not in the json response
      }, { transaction: t }).complete(fn);
    },

    setOfSavedLedgers: function(fn) {
      Ledger.findAll({
        attributes: ['id']
      }).complete(fn);
    }
  }
});

module.exports = Ledger;
