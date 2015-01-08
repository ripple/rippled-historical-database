var log        = require('../log')('hbase');
var moment     = require('moment');
var Promise    = require('bluebird');
var thrift     = require('thrift');
var HBase      = require('./hbase');
var HBaseTypes = require('./hbase_types');
var SerializedObject = require('ripple-lib').SerializedObject;
var binformat  = require('../../node_modules/ripple-lib/src/js/ripple/binformat');
var utils      = require('../utils');

var EPOCH_OFFSET = 946684800;
var LI_PAD       = 12;
var I_PAD        = 5;
var E_PAD        = 3;
var S_PAD        = 12;


/**
 * Client
 * HBase client class
 */

var Client = function (options) {
  this._retry      = 0;
  this._prefix     = options.prefix || '';
  this._host       = options.host;
  this._port       = options.port;
  this._timeout    = options.timeout || 10000;
  this._connection = null;
  
  this.hbase = null;
};

/**
 * isConnected
 */

Client.prototype.isConnected = function () {
  return !!this.hbase && !!this._connection && this._connection.connected;
};

/**
 * Connect
 * establish new thrift connection
 */

Client.prototype.connect = function () {
  var self = this;
  
  //check if connected already
  if (self.isConnected()) {
    return Promise.resolve();
  }

  //create new connection
  self._connection = thrift.createConnection(self._host, self._port, {
    transport : thrift.TFramedTransport,
    protocol  : thrift.TBinaryProtocol,
    connect_timeout : self._timeout
  });
    
  self._connection.on('connect', function() {
    self._retry = 0;
    self.hbase  = thrift.createClient(HBase,self._connection);
    log.info('hbase connected');
  });
            
  self._connection.on('error', function (err) {
    log.error('hbase error', err);
  }); 
    
  self._connection.on('close', function() {
    console.log('hbase connection closed');
    self._retryConnect(); //attempt to reconnect
  })
    
  //on the first connect, resolve the promise
  //if it errors before connecting the first 
  //time, fail the promise
  return new Promise (function(resolve, reject) {
    self._connection.once('connect', function() {
      resolve(true);
    });

    self._connection.once('error', function (err) {
      reject(err);
    });     
  }).catch(function(e) {
    log.error(e);
  });
};

/**
 * _retryConnect
 * attempt to reconnect when disconnected
 */

Client.prototype._retryConnect = function() {
  var self = this;

  this._retry += 1;

  var retryTimeout = (this._retry < 40)
  // First, for 2 seconds: 20 times per second
  ? (1000 / 20)
  : (this._retry < 40 + 60)
  // Then, for 1 minute: once per second
  ? (1000)
  : (this._retry < 40 + 60 + 60)
  // Then, for 10 minutes: once every 10 seconds
  ? (10 * 1000)
  // Then: once every 30 seconds
  : (30 * 1000);

  function connectionRetry() {
    self.connect();
  }

  if (this._retryTimeout) clearTimeout(this._retryTimeout);
  this._retryTimeout = setTimeout(connectionRetry, retryTimeout);
  log.info('attempting to reconnect: ', self._retry, retryTimeout);
};

/**
 * putRows
 * upload multiple rows for a single
 * table into HBase
 */

Client.prototype.putRows = function (table, rows) {
  var self   = this;
  var data   = [];
  var arrays = [];
  var columns;
  var name;
  var value;
  var chunk = 100;
  
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

  //chunk data at no more than 100 rows
  for (var i=0, j=data.length; i<j; i+=chunk) {
    arrays.push(data.slice(i,i+chunk));
  }

  //promiseify
  return Promise.map(arrays, function(chunk) {
    return new Promise (function(resolve, reject) {
      self.hbase.putMultiple(self._prefix + table, chunk, function(err, resp) {
        if (err) {
          log.error(self._prefix + table, err, resp);
          reject(err);
        } else {
          //console.log(self._prefix + table, "saved", chunk.length);
          resolve(resp);
        }
      });  
    });
  });
};

/**
 * putRow
 * save a single row
 */

Client.prototype.putRow = function (table, rowKey, data) {
  var self    = this;
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
    self.hbase.put(self._prefix + table, put, function(err, resp) {
      if (err) {
        log.info(self._prefix + table, err, resp);
        reject(err);
      } else {
        resolve(resp);
      }
    });   
  });
}

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
 * saveLedger
 * save a ledger to HBase
 */

Client.prototype.saveLedger = function (ledger, callback) {
  var self       = this;
  var tableNames = [];
  var tables     = {
    ledgers             : { },
    lu_ledgers_by_index : { },
    lu_ledgers_by_time  : { }
  };
  
  //adjust the close time to unix epoch
  ledger.close_time = ledger.close_time + EPOCH_OFFSET;
  
  var ledgerIndexKey = utils.padNumber(ledger.ledger_index, LI_PAD) + 
      '|' + ledger.ledger_hash;
  
  var ledgerTimeKey  = utils.formatTime(ledger.close_time) + 
      '|' + utils.padNumber(ledger.ledger_index, LI_PAD);
  
  //add formated ledger
  tables.ledgers[ledger.ledger_hash] = ledger;
  
  //add ledger index lookup
  tables.lu_ledgers_by_index[ledgerIndexKey] = {
    'f:ledger_index' : ledger.ledger_index,
    ledger_hash      : ledger.ledger_hash,
    parent_hash      : ledger.parent_hash,
    'f:close_time'   : ledger.close_time
  }
  
  //add ledger by time lookup
  tables.lu_ledgers_by_time[ledgerTimeKey] = {
    ledger_hash      : ledger.ledger_hash,
    parent_hash      : ledger.parent_hash,
    'f:ledger_index' : ledger.ledger_index,
    'f:close_time'   : ledger.close_time
  }
  
  tableNames = Object.keys(tables);
  
  Promise.map(tableNames, function(name) {
    return self.putRows(name, tables[name]);
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

module.exports = Client;