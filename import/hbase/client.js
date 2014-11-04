var config     = require('../../config/import.config');
var log        = require('../../lib/log')('hbase');
var Base       = require('ripple-lib').Base;
var parser     = require('../../lib/ledgerParser');
var moment     = require('moment');
var Promise    = require('bluebird');
var HBase      = require('hbase');

var PREFIX = "a2_";
var COMMON_FIELDS = [
  'Account',
  'AccountTxnID',
  'Fee',
  'Flags',
  'LastLedgerSequence',
  'Memos',
  'Sequence',
  'SigningPubKey',
  'SourceTag',
  'TransactionType',
  'TxnSignature',
  'ledger_hash',
  'ledger_index',
  'tx_index',
  'executed_time',
  'tx_result',
  'hash',
  'raw',
  'meta',
  'metaData'
];

/*
var baseUrl = 'http://54.164.78.183:20550';

*/
/*
HB.getTable(PREFIX + 'ledgers').create('data', function(err, success){});
HB.getTable(PREFIX + 'ledgers_by_index').create('data', function(err, success){});
HB.getTable(PREFIX + 'transactions')
.create({ColumnSchema: [
  {name: 'CommonFields'},
  {name: 'Payment'},
  {name: 'OfferCreate'},
  {name: 'OfferCancel'},
  {name: 'AccountSet'},
  {name: 'SetRegularKey'},
  {name: 'TrustSet'},
  {name: 'EnableAmendment'},
  {name: 'SetFee'}
]}, function(err, success){})

HB.getTable(PREFIX + 'transactions_by_index').create('data', function(err, success){});
HB.getTable(PREFIX + 'account_transactions').create('data', function(err, success){});
HB.getTable(PREFIX + 'account_transactions_by_index').create('data', function(err, success){});
HB.getTable(PREFIX + 'offers_exercised').create('data', function(err, success){});
HB.getTable(PREFIX + 'balance_changes').create('data', function(err, success){});
HB.getTable(PREFIX + 'accounts').create('data', function(err, success){});
*/

/**
 * Client
 * HBase client class
 */

var Client = function (options) { 
  var self    = this;
  self._ready = true;
  self._error = false;
  self._queue = [];
  self.hbase  = HBase({
    host : '54.164.78.183',
    port : 20550
  });
  
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
      addTable('ledgers_by_index'), 
      addTable('ledgers_by_time'), 
      addTable('transactions'),
      addTable('transactions_by_time'),
      addTable('transactions_by_account_sequence'),
      addTable('transactions_by_affected_account'),
      addTable('offers_exercised'),
      addTable('offers_exercised_by_account'),
      addTable('balance_changes'),
      addTable('balance_changes_by_account'),
      addTable('payments'),
      addTable('payments_by_account'),
      addTable('accounts_created'),
      addTable('accounts_created_by_parent'),     
      addTable('memos'),  
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
  
  function addTable (table, families) {
    if (!families) families = ['data'];
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
      
      //family = table === 'transactions' ? getColumnName(rows[rowkey].TransactionType, column) : 'data';
      family = 'data';
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
        reject(err);
      } else {
        resolve(resp);
      }
    });  
  });
  
  //determine column name 
  function getColumnName (family, column) {
    if (COMMON_FIELDS.indexOf(column) !== -1) {
      family = 'CommonFields';
    } 

    return family + ':' + column;
  }
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

/**
 * saveLedger
 * save a ledger and associated
 * transactions to HBase
 */

Client.prototype.saveLedger = function (ledger, callback) {
  var self = this;
  var data;
  var tables;
  
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
      

module.exports = new Client(config);