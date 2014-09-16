'use strict';

exports.up = function(knex, Promise) {
  return Promise.all([
    knex.schema.createTable('ledgers',function(table) {
      table.binary('hash').primary().unique();
      table.binary('index');
      table.binary('parent_hash');
      table.bigInteger('total_coins');
      table.bigInteger('close_time');
      table.bigInteger('close_time_resolution');
      table.dateTime('close_time_human');
      table.binary('account_hash');
      table.binary('transaction_hash');
    }),
    
    knex.schema.createTable('transactions', function(table) {
      table.binary('hash').primary();
      table.enu('type', [
        'Payment', 
        'OfferCreate', 
        'OfferCancel',
        'AccountSet', 
        'SetRegularKey',
        'TrustSet'  
      ]);
      table.binary('account');
      table.bigInteger('sequence');
      table.bigInteger('ledger_index');
      table.string('result');
      table.json('raw');
      table.json('meta');
    }),
    
    knex.schema.createTable('accounts', function(table) {
      table.binary('address').primary();
      table.binary('tx_hash');
      table.binary('parent');
      table.dateTime('created');
    })
/*    
    knex.schema.createTable('account_transactions', function(table) {
      table.binary('address');
      table.binary('tx_hash');
      table.bigInteger('ledger_index');
      table.bigInteger('sequence');
    });
*/
  ]);
};

exports.down = function(knex, Promise) {
  return Promise.all([
    knex.schema.dropTable('ledgers'),
    knex.schema.dropTable('transactions'),
    knex.schema.dropTable('accounts'),
    //knex.schema.dropTable('account_transactions'),
 
  ])  
};
