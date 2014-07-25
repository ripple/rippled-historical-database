var db = require('../sequelize.js');
var Sequelize = require('sequelize');

var Account = db.define('accounts', {
  address: Sequelize.BLOB
}, {
  timestamps: false,
  underscored: true
});

module.exports = Account;
