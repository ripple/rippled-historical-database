var config     = require('../../config/import.config');
var log        = require('../../lib/log')('hbase');
var Base       = require('ripple-lib').Base;
var parser     = require('../../lib/ledgerParser');
var moment     = require('moment');
var Promise    = require('bluebird');
var HBase      = require('hbase');

var PREFIX = "a3_";

/**
 * Client
 * HBase client class
 */

var Client = function (options) { 
  var self    = this;
  self._ready = true;
  self._error = false;
  self._queue = [];
  self.hbase  = HBase(config.get('hbase'));
  console.log(config.get('hbase'));
  /**
   * initTables
   * create tables and column families
   * if they do not exits
   */
  
  self._initTables = function (done) {
    self._ready = false;
    self._error = false; 

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
      var row;
      
      if (err) {
        log.error('Error configuring tables:', err);
        self._error = true;
      } else {
        self._ready = true;
        log.info('tables configured');
        
        //save queued ledgers
        while (self._queue.length) {
          row = self._queue.pop();
          self.saveLedger(row.ledger, row.callback);
        }
      }
    });
  }
  
  //ensure we have the proper tables before importing
  //self._initTables();
  
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

      self.hbase.getTable(PREFIX + table)
      .create({ColumnSchema : schema}, function(err, resp){ 
        if (err) {
          reject(err);
        } else {
          resolve(resp);
        }
      });  
    });  
  }
};


/**
 * getRow
 * get a row from a table
 */

Client.prototype.getRow = function (table, rowkey, columns) { 
};

/**
 * putRows
 * upload multiple rows for a single
 * table into HBase
 */

Client.prototype.putRows = function (table, rows) {
  var self = this;
  var data = [];
  var family;
  var columnName;
  var value;
  
  //format rows
  for (rowkey in rows) {    
    for (column in rows[rowkey]) {
      value = rows[rowkey][column];
      
      if (!value) {
        continue;
        
      } else if (typeof value !== 'string') {
        value = JSON.stringify(value);
      }
      
      family = 'd';
      data.push({
        key    : rowkey,
        column : family + ':' + column,
        $      : value
      });
    }
  }
  
  //only send it if we have data
  if (!data.length) {
    return null;
  }
  
  //promiseify
  return new Promise (function(resolve, reject) {
    self.hbase.getRow(PREFIX + table).put(data, function(err, resp){
      if (err) {
        console.log(PREFIX + table, err, resp);
        reject(err);
      } else {
        resolve(resp);
      }
    });  
  });
};

/**
 *
 */

Client.prototype.putRow = function (table, rowkey, family, data) {
  var self   = this;
  var fields = [];
  var values = [];
  var value;

  if (typeof family === 'object') {
    data   = family;
    family = null; 
  }
  
  if (!family) {
    family = 'data';
  }
  
  //format data
  for (key in data) {
    fields.push(family + ':' + key);
    value = data[key];
    if (typeof value !== 'string') {
      value = JSON.stringify(value);
    }
    
    values.push(value);
  }
  
  //promisify
  return new Promise (function(resolve, reject) {
    self.hbase.getRow(PREFIX + table, rowkey).put(fields, values, function(err, resp){
      if (err) {
        reject(err);
      } else {
        resolve(resp);
      }
    });  
  });
}

Client.prototype.saveRow = function (table, rowkey, data) {
  var self   = this;
  var fields = [];
  var values = [];
  var value;

  
  //format data
  data.forEach(function(column) {
    fields.push(column.family + ':' + column.name);
    value = column.value;
    if (typeof value !== 'string') {
      value = JSON.stringify(value);
    }
    
    values.push(value);
  });
  
  //promisify
  return new Promise (function(resolve, reject) {
    self.hbase.getRow(table, rowkey).put(fields, values, function(err, resp){
      if (err) {
        reject(err);
      } else {
        resolve(resp);
      }
    });  
  });
}

/**
 * saveLedger
 * save a ledger and associated
 * transactions to HBase
 */

Client.prototype.saveLedger = function (ledger, callback) {
  var self = this;
  var data;
  var tables;
  
  //make a copy
  ledger = JSON.parse(JSON.stringify(ledger));
  
  if (self._error) {
    return log.info('Ledger not saved:', ledger.ledger_index);
  }
  
  if (!self._ready) {
    return self._queue.push({ledger:ledger, callback:callback});
  }
  
  data   = parser.parseHBase(ledger); 
  //console.log(data);

  tables = Object.keys(data);

  
  Promise.map(tables, function(name) {
    return self.putRows(name, data[name]);
  })
  .nodeify(function(err, resp) {   
    if (err) {
      //TODO: log unsaved ledgers
      log.error('error saving ledger:', ledger.ledger_index, err);
    } else {
      log.info('ledger saved:', ledger.ledger_index);
    }
    
    if (callback) {
      callback(err, resp);
    }
  });
                
};
      

module.exports = new Client(config);