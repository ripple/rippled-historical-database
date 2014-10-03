'use strict';

exports.up = function(knex, Promise) {
  return Promise.all([
    knex.schema.table('account_transactions', function(table) {
      table.string('account', 64);
      table.binary('tx_hash');
    })
  ]);
};

exports.down = function(knex, Promise) {
  return Promise.all([
    knex.schema.table('account_transactions', function(table) {
      table.dropColumn('account');
      table.dropColumn('tx_hash');
    }) 
  ]);
};
