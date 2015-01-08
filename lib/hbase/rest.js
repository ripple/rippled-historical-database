var config    = require('../../config/import.config');
var HBaseRest = require('hbase');
var rest      = HBaseRest(config.get('hbase-rest'));

var Client = function () {
  /**
   * initTables
   * create tables and column families
   * if they do not exits
   */
  
  self._initTables = function (done) {

    Promise.all([    
      addTable('ledgers'),
      addTable('transactions'), 
      addTable('exchanges'), 
      addTable('balance_changes'),
      addTable('payments'),
      addTable('accounts_created'),
      addTable('memos'),
      addTable('lu_ledgers_by_index'),
      addTable('lu_ledgers_by_time'),
      addTable('lu_transactions_by_time'),
      addTable('lu_account_transactions'),
      addTable('lu_affected_account_transactions'),
      addTable('lu_account_exchanges'),
      addTable('lu_account_balance_changes'),
      addTable('lu_account_payments'),     
      addTable('lu_account_memos'),  
    ])
    .nodeify(function(err, resp) {
      
      if (err) {
        log.error('Error configuring tables:', err);
      } else {
        log.info('tables configured');
      }
    });
  }
  
  /**
   * addTable
   * add a new table to HBase
   */
  
  function addTable (table) {
    var families = ['f','d'];
    return new Promise (function(resolve, reject) {
      var schema = [];
      families.forEach(function(family) {
        schema.push({name : family});
      });

      rest.getTable(PREFIX + table)
      .create({ColumnSchema : schema}, function(err, resp){ 
        console.log(table, err, resp);
        if (err) {
          reject(err);
        } else {
          resolve(resp);
        }
      });  
    });  
  }
};

module.exports = new Client();