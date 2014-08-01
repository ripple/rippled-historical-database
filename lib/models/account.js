var db = require('../sequelize.js');
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
      Account.findAll({ where: { address: addresses }}).success(function(accounts) {
        var created_addresses = _.map(accounts, function(account) { return account.address.toString() });
        var needed_accounts = _.difference(addresses, created_addresses)
        needed_accounts = _.map(needed_accounts, function(address) { return { address: address }});

        if (needed_accounts.length > 0) {
          Account.bulkCreate(needed_accounts).success(function(created_accounts) {
            Account.findAll({ where: { address: addresses }}).success(function(accounts) {
              fn(null, accounts);
            });
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
