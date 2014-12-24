var config     = require('../../config/import.config');
var log        = require('../../lib/log')('hbase');
var Base       = require('ripple-lib').Base;
var parser     = require('../../lib/ledgerParser');
var moment     = require('moment');
var Promise    = require('bluebird');
var thrift     = require('thrift');
var HBase      = require('../../lib/hbase/hbase');
var HBaseTypes = require('../../lib/hbase/hbase_types');
var HBaseRest  = require('hbase');
var dbConfig   = config.get('hbase');  
 
var connection = thrift.createConnection(dbConfig.host, dbConfig.port, {
  transport : thrift.TFramedTransport,
  protocol  : thrift.TBinaryProtocol
});

var PREFIX = "test_a3_";
var hbase;
var rest = HBaseRest(config.get('hbase-rest'));

connection.on('connect', function() {
  hbase = thrift.createClient(HBase,connection);
});

/**
 * Client
 * HBase client class
 */

var Client = function () { 
  var self    = this;
  self._ready = true;
  self._error = false;
  self._queue = [];

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

      rest.getTable(PREFIX + table)
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
  var columns;
  var name;
  var value;
  
  if (!hbase) {
    throw new Error('hbase not connected');
    return;
  }
  
  //format rows
  for (rowKey in rows) { 
    columns = prepareColumns(rows[rowKey]);
    
    if (!columns.length) {
      continue;
    }
    
    //if (table === 'transactions')
    //console.log(table, rowKey, columns);
    data.push(new HBaseTypes.TPut({
      row          : rowKey,
      columnValues : columns
    }));
  }
  
  //only send it if we have data
  if (!data.length) {
    return null;
  }
  
  //promiseify
  return new Promise (function(resolve, reject) {
    hbase.putMultiple(PREFIX + table, data, function(err, resp) {
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
 * putRow
 * save a single row
 */

Client.prototype.putRow = function (table, rowkey, data) {
  var self   = this;
  var columns = prepareColumns(data);
  var put;
  
  if (!columns.length) {
    return;
  }
  
  
  put = new HBaseTypes.TPut({
    row          : rowKey,
    columnValues : columns
  });
  
  //promisify
  return new Promise (function(resolve, reject) {
    hbase.put(PREFIX + table, put, function(err, resp) {
      if (err) {
        console.log(PREFIX + table, err, resp);
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

/**
 * prepareColumns
 * create an array of columnValue
 * objects for the given data
 */

function prepareColumns (data) {
  var columns = [];
  
  for (column in data) {
    value = data[column];

    //ignore empty rows
    if (!value) {
      continue;
    }

    columns.push(prepareColumn(column, value));
  }
  
  return columns;
}

/**
 * prepareColumn
 * create a columnValue object
 * for the given column
 */

function prepareColumn (key, value) {
  var name;
  
  //stringify JSON and arrays  
  if (typeof value !== 'string') {
    value = JSON.stringify(value);
  }

  //default family to 'd' for data
  name  = key.split(':');
  return new HBaseTypes.TColumnValue({
    family    : name[1] ? name[0] : 'd',
    qualifier : name[1] ? name[1] : name[0],
    value     : value
  });
}

module.exports = new Client();