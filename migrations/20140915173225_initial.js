'use strict';

exports.up = function(knex, Promise) {
  return Promise.all([

    knex.schema.createTable('ledgers',function(table) {
      table.bigIncrements('ledger_id').primary().unsigned();
      table.integer('ledger_index');
      table.binary('ledger_hash').unique();
      table.binary('parent_hash').unique();
      table.bigInteger('total_coins');
      table.bigInteger('close_time');
      table.bigInteger('close_time_resolution');
      table.binary('accounts_hash');
      table.binary('transactions_hash');
    }),

    knex.schema.createTable('transactions', function(table) {
      table.bigIncrements('tx_id').primary().unsigned();
      table.binary('tx_hash').unique();
      table.bigInteger('ledger_id').references('ledger_id').inTable('ledgers');
      table.bigInteger('ledger_index');
      table.integer('tx_seq');
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
      table.string('account', 64);
      table.bigInteger('account_seq');
      table.string('tx_result');
      table.binary('tx_raw');
      table.binary('tx_meta');
      table.bigInteger('executed_time');
    }),

    knex.schema.createTable('accounts', function(table) {
      table.bigIncrements('account_id').primary().unsigned();
      table.string('account', 64).unique();
      table.string('parent', 64);      
      table.binary('tx_hash');
      table.bigInteger('created_time');
    }),
    
  ]).then(function(){
    return knex.schema.createTable('account_transactions', function(table) {
      table.bigInteger('account_id').references('account_id').inTable('accounts');
      table.bigInteger('tx_id').references('tx_id').inTable('transactions');
      table.primary(['tx_id', 'account_id']);
      table.string('account', 64);
      table.binary('tx_hash');
      table.bigInteger('ledger_index');
      table.integer('tx_seq');
    })
  });
};

exports.down = function(knex, Promise) {
  return Promise.all([ 
    knex.raw('DROP TABLE IF EXISTS account_transactions; ' + 
      'DROP TABLE IF EXISTS ledgers CASCADE; ' + 
      'DROP TABLE IF EXISTS transactions CASCADE; ' + 
      'DROP TABLE IF EXISTS accounts')
  ]); 
};
