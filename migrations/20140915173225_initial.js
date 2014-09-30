'use strict';

exports.up = function(knex, Promise) {
  return Promise.all([

    knex.schema.createTable('ledgers',function(table) {
      table.integer('ledger_index').primary().unique();
      table.binary('ledger_hash');
      table.binary('parent_hash');
      table.bigInteger('total_coins');
      table.bigInteger('close_time');
      table.bigInteger('close_time_resolution');
      table.binary('accounts_hash');
      table.binary('transactions_hash');
    }),

    knex.schema.createTable('transactions', function(table) {
      table.bigIncrements('tx_id').primary().unsigned();
      table.binary('tx_hash').unique();
      table.enu('tx_type', [
        'Payment',
        'OfferCreate',
        'OfferCancel',
        'AccountSet',
        'SetRegularKey',
        'TrustSet',
        'EnableAmendment',
        'SetFee' 
      ]);
      table.binary('account');
      table.bigInteger('account_seq');
      table.integer('tx_seq');
      table.bigInteger('ledger_index').references('ledger_index').inTable('ledgers');
      table.string('result');
      table.binary('tx_raw');
      table.binary('tx_meta');
      table.bigInteger('executed_time');
    }),

    knex.schema.createTable('accounts', function(table) {
      table.bigIncrements('account_id').primary().unsigned();
      table.binary('account').unique();
      table.binary('tx_hash');
      table.binary('parent');
      table.bigInteger('created_time');
    }),

    knex.schema.createTable('account_transactions', function(table) {
      table.bigInteger('account_id').references('account_id').inTable('accounts');
      table.bigInteger('tx_id').references('tx_id').inTable('transactions');
    })
   
  ]);
};

exports.down = function(knex, Promise) {
  return Promise.all([ 
    knex.raw('DROP TABLE IF EXISTS account_transactions; ' + 
      'DROP TABLE IF EXISTS ledgers CASCADE; ' + 
      'DROP TABLE IF EXISTS transactions CASCADE; ' + 
      'DROP TABLE IF EXISTS accounts')
  ]); 
};
