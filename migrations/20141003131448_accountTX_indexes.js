'use strict';

exports.up = function(knex, Promise) {
  return Promise.all([
    knex.schema.table('ledgers', function(table) {
      table.index(['parent_hash'],  'parent_hash_idx');
      table.index(['ledger_index'], 'ledger_index_idx');
      table.index(['closing_time'],   'closing_time_idx');
    }),
    
    knex.schema.table('transactions', function(table) {
      table.index(['ledger_hash'], 'tx_ledger_hash_idx');
      table.index(['executed_time'], 'tx_time_idx');
      table.index(['tx_result'], 'tx_result_idx');
      table.index(['tx_type'], 'tx_type_idx');
      table.index(['account', 'account_seq'], 'tx_account_seq_idx');   
    }),
    
    knex.schema.table('account_transactions', function(table) {
      table.index(['executed_time'], 'account_tx_time_idx');
      table.index(['tx_result'], 'account_tx_result_idx');
      table.index(['tx_type'], 'account_tx_type_idx');
    }),
    
    knex.raw('CREATE INDEX transactions_ledger_index_tx_seq ON transactions (ledger_index DESC, tx_seq DESC)'),   
    knex.raw('CREATE INDEX account_transactions_account_ledger_index_tx_seq_idx ON account_transactions (account, ledger_index DESC, tx_seq DESC)'),
  ]);
};

exports.down = function(knex, Promise) {
  return Promise.all([
    knex.raw('drop index if exists "ledger_index_idx"'),
    knex.raw('drop index if exists "close_time_idx"'),
    knex.raw('drop index if exists "tx_ledger_id_idx"'), 
    knex.raw('drop index if exists "tx_type_idx"'),
    knex.raw('drop index if exists "tx_result_idx"'),
    knex.raw('drop index if exists "acccount_tx_idx"'),
    knex.raw('drop index if exists "acccount_idx"'),
    knex.raw('drop index if exists "tx_ledger_index_seq_idx"'),
    knex.raw('drop index if exists "tx_time_seq_idx"'),
    knex.raw('drop index if exists "account_transactions_account_ledger_index_tx_seq_tx_hash_idx"'),
  ]); 
};
