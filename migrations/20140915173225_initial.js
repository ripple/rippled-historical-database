'use strict';

exports.up = function(knex, Promise) {
  return Promise.all([
    knex.schema.createTable('ledgers',function(table) {
      table.bigInteger('id').primary();
      table.binary('ledger_hash').nullable();
      table.binary('parent_hash');
      table.bigInteger('total_coins');
      table.bigInteger('close_time');
      table.bigInteger('close_time_resolution');
      table.binary('account_hash');
      table.binary('transaction_hash');
      table.dateTime('close_time_human');
    }),
    knex.schema.createTable('transactions', function(table) {
      table.bigint('id').primary();
      table.binary('account');
      table.bigInteger('fee');
      table.bigInteger('flags');
      
    })
  ]);
};

exports.down = function(knex, Promise) {
  return Promise.all([
    knex.schema.dropTable('ledgers'),
    knex.schema.dropTable('transactions'),
    knex.schema.dropTable('ledger_transactions'),
    knex.schema.dropTable('accounts'),
  ])  
};
