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
  var self  = this;

  this.max_sockets = options.max_sockets || 1000;
  this._prefix     = options.prefix || '';
  this._host       = options.host;
  this._port       = options.port;
  this._timeout    = options.timeout || 30000; //also acts as keepalive
  this._connection = null;
  this.hbase       = null;
  this.log         = new Logger({
    scope : 'hbase-thrift',
    level : options.logLevel || 0,
    file  : options.logFile
  });

  this.pool = [ ];
};

HbaseClient.prototype._getConnection = function(cb) {
  var self = this;
  var connection;
  var hbase;
  var i = self.pool.length;

  //look for a free socket
  while (i--) {
    if (self.pool[i].client &&
        self.pool[i].connected &&
        !Object.keys(self.pool[i].client._reqs).length &&
        !self.pool[i].keep) {

      //console.log(self.pool.length, i);
      cb(null, self.pool[i]);
      return;
    }
  }

  //open a new socket if there is room in the pool
  if (self.pool.length < self.max_sockets) {
    openNewSocket();
  }

  //recheck for connected socket
  setTimeout(function() {
    self._getConnection(cb);
  }, 20);

  return;

  /**
   * openNewSocket
   */

  function openNewSocket() {
    var index;

    //create new connection
    connection = thrift.createConnection(self._host, self._port, {
      transport : thrift.TFramedTransport,
      protocol  : thrift.TBinaryProtocol,
      timeout   : self._timeout
    });


    //handle errors
    connection.error = function (err) {
      this.connected = false;
      delete self.pool[this.pool_index];
      self.pool.splice(this.pool_index, 1);

      for (var key in this.client._reqs) {
        this.client._reqs[key](err);
        delete (this.client._reqs[key]);
      }

      this.connection.destroy();
    };

    connection.pool_index = self.pool.push(connection) - 1;

    connection.on('timeout', function() {
      this.error('thrift connection timeout');
    });

    connection.once('connect', function() {
      this.client = thrift.createClient(HBase, connection);
    });

    connection.on('error', function (err) {
      this.error('thrift connection error: ' + err);
    });

    connection.on('close', function() {
      this.error('hbase connection closed');
    })
  }
}

/**
 * iterator
 * iterate through a scan, one at a time
 */

HbaseClient.prototype.iterator = function (options) {
  var self     = this;
  var table    = self._prefix + options.table;
  var scanOpts = { };
  var scan;
  var scan_id;
  var error;
  var count = 0;
  var total = 0;
  //create scan
  self._getConnection(function(err, connection) {

    if (err) {
      callback(err);
      return;
    }

    //invert stop and start index
    if (options.descending === false) {
      scanOpts.startRow = options.stopRow  ? options.stopRow.toString()  : undefined;
      scanOpts.stopRow  = options.startRow ? options.startRow.toString() : undefined;

    } else {
      scanOpts.stopRow  = options.stopRow  ? options.stopRow.toString()  : undefined;
      scanOpts.startRow = options.startRow ? options.startRow.toString() : undefined;
      scanOpts.reversed = true;
    }

    scan = new HBaseTypes.TScan(scanOpts);

    connection.client.scannerOpenWithScan(table, scan, null, function(err, id) {

      if (err) {
        self.log.error("unable to create scanner", err);
        error = err;
      }

      scan_id = id;
    });
  });

  self.getNext = function(callback) {

    if (!scan_id && !error) {
      setTimeout(function() {
        self.getNext(callback);
      }, 50);
      return;
    };

    //get connection
    self._getConnection(function(err, connection) {

      if (err) {
        callback(err);
        return;
      }

      connection.client.scannerGet(scan_id, function (err, rows) {
        var results = [];
        var key;
        var parts;
        var r;

        if (err) {
          callback(err);
          return;
        }

        //format as json
        results = formatRows(rows || []);
        callback(null, results[0]);
      });
    });
  };

  self.close = function () {

    if (!scan_id) return;

    //close the scanner
    self._getConnection(function(err, connection) {

      if (err) {
        self.log.error('connection error:', err);
        return;
      }

      connection.client.scannerClose(scan_id, function(err, resp) {
        if (err) {
          self.log.error('error closing scanner:', err);
        }
      });
    });
  };

  return this;
};

/**
 * getScan
 */

HbaseClient.prototype.getScan = function (options, callback) {
  var self     = this;
  var table    = self._prefix + options.table;
  var scanOpts = { };
  var scan;
  var swap;

  //get connection
  self._getConnection(function(err, connection) {

    if (err) {
      callback(err);
      return;
    }

    //keep till we are finished
    connection.keep = true;

    //default to reversed,
    //invert stop and start index
    if (options.descending === false) {
      scanOpts.startRow = options.stopRow.toString();
      scanOpts.stopRow  = options.startRow.toString();

      if (scanOpts.startRow > scanOpts.stopRow) {
        swap              = scanOpts.startRow;
        scanOpts.startRow = scanOpts.stopRow;
        scanOpts.stopRow  = swap;
      }

    } else {
      scanOpts.stopRow  = options.stopRow.toString();
      scanOpts.startRow = options.startRow.toString();
      scanOpts.reversed = true;

      if (scanOpts.startRow < scanOpts.stopRow) {
        swap              = scanOpts.startRow;
        scanOpts.startRow = scanOpts.stopRow;
        scanOpts.stopRow  = swap;
      }
    }

    scan = new HBaseTypes.TScan(scanOpts);

    connection.client.scannerOpenWithScan(table, scan, null, function(err, id) {

      if (err) {
        self.log.error(err);
        callback('unable to create scanner');
        connection.keep = false;
        return;
      }

      getResults(id, options.limit, 1, function (err, rows) {

        callback(err, rows);

        //close the scanner
        connection.client.scannerClose(id, function(err, resp) {
          if (err) {
            self.log.error('error closing scanner:', err);
          }
          //release
          connection.keep = false;
        });
      });
    });

    function getResults (id, limit, page, callback) {
      var count = limit && limit < 1000 ? limit : 1000;

      connection.client.scannerGetList(id, count, function (err, rows) {
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

  });
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

  //chunk data at no more than 100 rows
  for (var i=0, j=data.length; i<j; i+=chunk) {
    arrays.push(data.slice(i,i+chunk));
  }

  //promiseify
  return Promise.map(arrays, function(chunk) {
    return new Promise (function(resolve, reject) {
      self.log.info(table, '- saving ' + chunk.length + ' rows');
      self._getConnection(function(err, connection) {

        if (err) {
          callback(err);
          return;
        }

        connection.client.mutateRows(self._prefix + table, chunk, null, function(err, resp) {
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

  //promisify
  return new Promise (function(resolve, reject) {
    self._getConnection(function(err, connection) {

      if (err) {
        callback(err);
        return;
      }

      connection.client.mutateRow(self._prefix + table, rowKey, columns, null, function(err, resp) {
        if (err) {
          self.log.error(self._prefix + table, err, resp);
          reject(err);

        } else {
          resolve(resp);
        }
      });
    });
  });
}

HbaseClient.prototype.getRow = function (table, rowkey, callback) {
  var self = this;
  self._getConnection(function(err, connection) {

    if (err) {
      callback(err);
      return;
    }

    connection.client.getRow(self._prefix + table, rowkey, null, function (err, rows) {
      var row = null;

      if (rows) {
        rows = formatRows(rows);
        row  = rows[0];
      }

      callback(err, row);
    });
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
