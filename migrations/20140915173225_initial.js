'use strict';

exports.up = function(knex, Promise) {
  return Promise.all([
    knex.schema.createTable('ledgers',function(table) {
      table.binary('hash').unique();
      table.binary('index').unique();
      table.binary('parent_hash');
      table.bigInteger('total_coins');
      table.bigInteger('close_time');
      table.bigInteger('close_time_resolution');
      table.dateTime('close_time_human');
      table.binary('account_hash');
      table.binary('transaction_hash');
    }),
    
    knex.schema.createTable('transactions', function(table) {
      table.bigIncrements('id').primary().unsigned();
      table.binary('hash').unique();
      table.enu('type', [
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
      table.bigInteger('sequence');
      table.bigInteger('ledger_index').references('index').inTable('ledgers');
      table.string('result');
      table.binary('raw');
      table.binary('meta');
    }),
    
    knex.schema.createTable('accounts', function(table) {
      table.bigIncrements('id').primary().unsigned();
      table.binary('address').unique();
      table.binary('tx_hash');
      table.binary('parent');
      table.dateTime('created');
    }),
    
    knex.schema.createTable('account_transactions', function(table) {
      table.bigInteger('account_id').references('id').inTable('accounts');
      table.bigInteger('transaction_id').references('id').inTable('transactions');
    })
  ]);
};

exports.down = function(knex, Promise) {
  return Promise.all([
    knex.schema.dropTable('ledgers'),
    knex.schema.dropTable('transactions'),
    knex.schema.dropTable('accounts'),
    knex.schema.dropTable('account_transactions'),
  ]);  
};
