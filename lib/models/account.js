var db = require('../sequelize.js').db;
var Sequelize = require('sequelize');
var _ = require('underscore');

var Account = db.define('accounts', {
  id: {
    type: Sequelize.BIGINT,
    autoIncrement: true,
    primaryKey: true
  },
  address: Sequelize.BLOB
}, {
  timestamps: false,
  underscored: true,

  classMethods: {
    findOrCreate: function(addresses, fn) {
      Account.findAll({ where: { address: addresses }}).complete(function(error, accounts) {
        if (error) {
          fn(error);
          return;
        }
        var created_addresses = _.map(accounts, function(account) { return account.address.toString() });
        var needed_accounts = _.difference(addresses, created_addresses)
        needed_accounts = _.map(needed_accounts, function(address) { return { address: address }});

        if (needed_accounts.length > 0) {
          Account.bulkCreate(needed_accounts).complete(function(error, created_accounts) {
            if (error) {
              fn(error);
              return;
            }
            // Must findAll again to get full object with an id
            Account.findAll({ where: { address: addresses }}).complete(fn);
          });
        }
        else {
          fn(null, accounts);
        }
      });
    }
  }
});

module.exports = Account;
