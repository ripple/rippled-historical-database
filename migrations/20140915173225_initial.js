'use strict';

exports.up = function(knex, Promise) {
  return Promise.all([
    knex.schema.createTable('ledgers',function(table) {
      table.binary('ledger').primary();
      table.integer('ledger_index');
      table.binary('parent_hash');
      table.bigInteger('total_coins');
      table.bigInteger('closing_time');
      table.bigInteger('close_time_res');
      table.binary('accounts_hash');
      table.binary('transactions_hash');
    }),

    knex.schema.createTable('transactions', function(table) {
      table.binary('tx_hash').primary();
      table.binary('tx_raw');
      table.binary('tx_meta');
      table.binary('ledger_hash');
      table.bigInteger('ledger_index');
      table.integer('tx_seq');
      table.bigInteger('executed_time');
      table.enum('tx_result', [
        'tesSUCCESS',
        'tecCLAIM',
        'tecPATH_PARTIAL',
        'tecUNFUNDED_ADD',
        'tecUNFUNDED_OFFER',
        'tecUNFUNDED_PAYMENT',
        'tecFAILED_PROCESSING',
        'tecDIR_FULL',
        'tecINSUF_RESERVE_LINE',
        'tecINSUF_RESERVE_OFFER',
        'tecNO_DST',
        'tecNO_DST_INSUF_XRP',
        'tecNO_LINE_INSUF_RESERVE',
        'tecNO_LINE_REDUNDANT',
        'tecPATH_DRY',
        'tecUNFUNDED',
        'tecMASTER_DISABLED',
        'tecNO_REGULAR_KEY',
        'tecOWNERS',
        'tecNO_ISSUER',
        'tecNO_AUTH',
        'tecNO_LINE',
        'tecINSUFF_FEE',
        'tecFROZEN',
        'tecNO_TARGET',
        'tecNO_PERMISSION',
        'tecNO_ENTRY',
        'tecINSUFFICIENT_RESERVE'
      ]);
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
      table.integer('account_seq');
    })
  ]).then(function(){
    
    return knex.schema.createTable('account_transactions', function(table) {
      table.binary('account');
      table.binary('tx_hash').references('tx_hash').inTable('transactions');
      table.primary(['tx_hash','account']);
      table.bigInteger('ledger_index');
      table.integer('tx_seq');
      table.bigInteger('executed_time');
      table.enum('tx_result', [
        'tesSUCCESS',
        'tecCLAIM',
        'tecPATH_PARTIAL',
        'tecUNFUNDED_ADD',
        'tecUNFUNDED_OFFER',
        'tecUNFUNDED_PAYMENT',
        'tecFAILED_PROCESSING',
        'tecDIR_FULL',
        'tecINSUF_RESERVE_LINE',
        'tecINSUF_RESERVE_OFFER',
        'tecNO_DST',
        'tecNO_DST_INSUF_XRP',
        'tecNO_LINE_INSUF_RESERVE',
        'tecNO_LINE_REDUNDANT',
        'tecPATH_DRY',
        'tecUNFUNDED',
        'tecMASTER_DISABLED',
        'tecNO_REGULAR_KEY',
        'tecOWNERS',
        'tecNO_ISSUER',
        'tecNO_AUTH',
        'tecNO_LINE',
        'tecINSUFF_FEE',
        'tecFROZEN',
        'tecNO_TARGET',
        'tecNO_PERMISSION',
        'tecNO_ENTRY',
        'tecINSUFFICIENT_RESERVE'
      ]);
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
