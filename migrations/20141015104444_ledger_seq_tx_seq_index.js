'use strict';

exports.up = function(knex, Promise) {
  knex.schema.table('transactions', function(table) {
    table.index(['ledger_index', 'tx_seq'], 'tx_ledger_index_tx_seq_idx')
  }); 
};

exports.down = function(knex, Promise) {
  knex.raw('drop index if exists "tx_ledger_index_tx_seq_idx"');   
};
