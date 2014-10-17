'use strict';

exports.up = function(knex, Promise) {
  return Promise.all([
    knex.raw('CREATE INDEX tx_ledger_index_tx_seq_idx ON transactions (ledger_index DESC, tx_seq DESC)'),
    knex.raw('drop index if exists "account_tx_idx"') 
  ]).then(function(){
    return knex.raw('ALTER TABLE account_transactions ADD PRIMARY KEY (tx_id, account_id)');
  });
};

exports.down = function(knex, Promise) {
  return Promise.all([
    knex.raw('drop index if exists "tx_ledger_index_tx_seq_idx"'),
    knex.raw('ALTER TABLE account_transactions DROP CONSTRAINT account_transactions_pkey')
  ]).then(function(){
    return knex.schema.table('account_transactions', function(table) {
      table.index(['account_id','tx_id'], 'account_tx_idx');
    });
  });  
};
