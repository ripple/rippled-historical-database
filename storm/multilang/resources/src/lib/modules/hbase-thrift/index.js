var Promise    = require('bluebird');
var thrift     = require('thrift');
var HBase      = require('./gen/Hbase');
var HBaseTypes = require('./gen/Hbase_types');
var Logger     = require('../logger');


/**
 * HbaseClient
 * HBase client class
 */

var HbaseClient = function (options) {
  this._retry      = 0;
  this._prefix     = options.prefix || '';
  this._host       = options.host;
  this._port       = options.port;
  this._timeout    = options.timeout || 10000;
  this._connection = null;
  this.hbase       = null;
  this.log         = new Logger({
    scope : 'hbase-thrift',
    level : options.logLevel || 0,
    file  : options.logFile
  });
};

/**
 * isConnected
 */

HbaseClient.prototype.isConnected = function () {
  return !!this.hbase && !!this._connection && this._connection.connected;
};

/**
 * Connect
 * establish new thrift connection
 */

HbaseClient.prototype.connect = function () {
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
    self.log.info('hbase connected');
  });
            
  self._connection.on('error', function (err) {
    self.log.error('hbase error', err);
  }); 
    
  self._connection.on('close', function() {
    self.log.info('hbase connection closed');
    self._retryConnect(); //attempt to reconnect
  })
    
  //on the first connect, resolve the promise
  //if it errors before connecting the first 
  //time, fail the promise
  return new Promise (function(resolve, reject) {
    self._connection.once('connect', function() {
      self.hbase = thrift.createClient(HBase,self._connection);
      resolve(true);
    });

    self._connection.once('error', function (err) {
      reject(err);
    });     
  }).catch(function(e) {
    return Promise.reject(e);
  });
};

/**
 * _retryConnect
 * attempt to reconnect when disconnected
 */

HbaseClient.prototype._retryConnect = function() {
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
    self.log.info('retry connect');
    self.connect();
  }

  if (this._retryTimeout) clearTimeout(this._retryTimeout);
  this._retryTimeout = setTimeout(connectionRetry, retryTimeout);
};

HbaseClient.prototype.getScan = function (options, callback) {
  var self     = this;
  var table    = self._prefix + options.table;
  var scanOpts = { };
  var scan;
  
  //check connection
  if (!self.isConnected()) {
    callback('not connected');
    return;
  }
  
  //default to reversed, 
  //invert stop and start index
  if (options.descending === false) {
    scanOpts.startRow = options.stopRow.toString();
    scanOpts.stopRow  = options.startRow.toString();
    scanOpts.reversed = true;
  
  } else {
    scanOpts.stopRow  = options.stopRow.toString();
    scanOpts.startRow = options.startRow.toString();   
  }
  
  scan = new HBaseTypes.TScan(scanOpts);
  
  self.hbase.scannerOpenWithScan(table, scan, null, function(err, id) {

    if (err) {
      self.log.error(err);
      callback('unable to create scanner');
      return;
    }
    
    getResults(id, options.limit, 1, function (err, rows) {
      
      callback(err, rows);
      
      //close the scanner
      self.hbase.scannerClose(id, function(err, resp) {
        if (err) {
          self.log.error('error closing scanner:', err);
        }
      });
    });
  });
  
  function getResults (id, limit, page, callback) {
    var count = limit && limit < 1000 ? limit : 1000;
    
    self.hbase.scannerGetList(id, count, function (err, rows) {
      var results = [];
      var key;
      var parts;
      var r;
      
      if (err) {
        callback(err);
        return;
      }
      
      if (rows.length) {
        
        //format as json
        results = formatRows(rows);
        
        //stop if we are at the limit
        if (limit && page * count >= limit) {
          callback (null, results);
          
        } else {
          
          //recursively get more results
          getResults(id, limit, ++page, function(err, rows) {
            results.push.apply(results, rows);
            callback(null, results);    
          });
        } 
      
      } else {
        callback (null, []);
      }
    });
  }
};

/**
 * putRows
 * upload multiple rows for a single
 * table into HBase
 */

HbaseClient.prototype.putRows = function (table, rows) {
  var self   = this;
  var data   = [];
  var arrays = [];
  var columns;
  var name;
  var value;
  var chunk = 100;
  
  
  //format rows
  for (rowKey in rows) { 
    columns = self._prepareColumns(rows[rowKey]);
    
    if (!columns.length) {
      continue;
    }
                  
    data.push(new HBaseTypes.BatchMutation({
      row       : rowKey,
      mutations : columns
    }));
  }
  
  //only send it if we have data
  if (!data.length) {
    return Promise.resolve();
  }
  
  //check connection
  if (!self.isConnected()) {
    return Promise.reject('not connected');
  }

  //chunk data at no more than 100 rows
  for (var i=0, j=data.length; i<j; i+=chunk) {
    arrays.push(data.slice(i,i+chunk));
  }

  //promiseify
  return Promise.map(arrays, function(chunk) {
    return new Promise (function(resolve, reject) {
      self.log.info(table, '- saving ' + chunk.length + ' rows');
      self.hbase.mutateRows(self._prefix + table, chunk, null, function(err, resp) {
        if (err) {
          self.log.error(self._prefix + table, err, resp);
          reject(err);
        } else {
          //self.log.info(self._prefix + table, "saved", chunk.length);
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

HbaseClient.prototype.putRow = function (table, rowKey, data) {
  var self    = this;
  var columns = self._prepareColumns(data);
  var put;
  
  if (!columns.length) {
    return Promise.resolve();
  }

  //check connection
  if (!self.isConnected()) {
    return Promise.reject('not connected');
  }
  
  //promisify
  return new Promise (function(resolve, reject) {
    self.hbase.mutateRow(self._prefix + table, rowKey, columns, null, function(err, resp) {
      if (err) {
        self.log.error(self._prefix + table, err, resp);
        reject(err);
      } else {
        resolve(resp);
      }
    });   
  });
}

HbaseClient.prototype.getRow = function (table, rowkey, callback) { 
  var self = this;
  
  //check connection
  if (!self.isConnected()) {
    callback('not connected');
    return;
  }
  
  self.hbase.getRow(self._prefix + table, rowkey, null, function (err, rows) {
    var row = null;
    
    if (rows) {
      rows = formatRows(rows);
      row  = rows[0];
    }
    
    callback(err, row);
  });
};

/**
 * prepareColumns
 * create an array of columnValue
 * objects for the given data
 */

HbaseClient.prototype._prepareColumns = function (data) {
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
  
  /**
   * prepareColumn
   * create a columnValue object
   * for the given column
   */

  function prepareColumn (key, value) {
    var name;
    var column;
    
    //stringify JSON and arrays  
    if (typeof value !== 'string') {
      value = JSON.stringify(value);
    }

    //default family to 'd' for data
    name    = key.split(':');
    column  = name[1] ? name[0] : 'd';
    column += ':' + (name[1] ? name[1] : name[0]);
    
    return new HBaseTypes.Mutation({
      column    : column,
      value     : value
    });
  }
};

                    
function formatRows(data) {
  var rows = [];
  data.forEach(function(row) {
    r = {};
    r.rowkey = row.row;
    for (key in row.columns) {
      parts = key.split(':');
      r[parts[1]] = row.columns[key].value;
    }

    rows.push(r);
  }); 
  
  return rows;
}

module.exports = HbaseClient;