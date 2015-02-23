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

  this.max_sockets = options.max_sockets || 300;
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

  this.pool = [ ];

  /**
   * purgePool
   */

  function purgePool () {
    var i   = self.pool.length;
    var now = Date.now();
    var age;

    while (i--) {

      //flagged for removal
      if (self.pool[i].remove) {
        self.pool.splice(i, 1);

      } else {
        age = now - self.pool[i].last;

        //keep alive at least 30 seconds
        if (self.pool[i].free && age > 30000) {
          self.pool.splice(i, 1);

        //max at 10 minutes
        } else if (age > 60 * 1000 * 5) {
          self.pool.splice(i, 1);
        }
      }
    }
  }

  setInterval(purgePool, 30000);
};

HbaseClient.prototype.getConnection = function(cb) {
  var self = this;
  var connection;
  var hbase;
  var i = self.pool.length;

  //look for a free socket
  while (i--) {
    if (self.pool[i].free && self.pool[i].connected) {
      self.pool[i].free = false;
      self.pool[i].last = Date.now();
      cb(null, self.pool[i]);
      return;

    } else if (self.pool[i].remove) {
      self.pool.splice(i, 1);
    }
  }

  //open a new socket if there is room in the pool
  if (self.pool.length < self.max_sockets) {
    openNewSocket();

  //wait and check again
  } else {
    setTimeout(function() {
      self.getConnection(cb);
    }, 20);
  }

  /**
   * openNewSocket
   */

  function openNewSocket() {

    connection = thrift.createConnection(self._host, self._port, {
      transport : thrift.TFramedTransport,
      protocol  : thrift.TBinaryProtocol,
      connect_timeout : self._timeout
    });

    /**
     * release socket
     * release for another use or removal
     */

    connection.release = function (err) {

      //set to be removed from the pool
      if (err) {
        this.remove    = true;
        this.connected = false;

      //make available for reuse
      } else {
        this.free = true;
      }

      this.last = Date.now();
    };

    connection.free  = false;
    self.pool.push(connection);

    connection.once('connect', function() {
      connection.client = thrift.createClient(HBase, connection);
      connection.last   = Date.now();
      cb(null, connection);
    });

    connection.once('error', function (err) {
      self.log.error('thrift connection error', err);
      connection.release(true);
      cb(err);
    });

    connection.once('close', function() {
      self.log.info('hbase connection closed');
      connection.release(true);
    })
  }
}

/**
 * getScan
 */

HbaseClient.prototype.getScan = function (options, callback) {
  var self     = this;
  var table    = self._prefix + options.table;
  var scanOpts = { };
  var scan;

  //get connection
  self.getConnection(function(err, connection) {

    if (err) {
      callback(err);
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

    connection.client.scannerOpenWithScan(table, scan, null, function(err, id) {

      if (err) {
        self.log.error(err);
        callback('unable to create scanner');
        connection.release(err);
        return;
      }

      getResults(id, options.limit, 1, function (err, rows) {

        callback(err, rows);

        //close the scanner
        connection.client.scannerClose(id, function(err, resp) {
          if (err) {
            self.log.error('error closing scanner:', err);
          }
          connection.release(err);
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
      self.getConnection(function(err, connection) {

        if (err) {
          callback(err);
          return;
        }

        connection.client.mutateRows(self._prefix + table, chunk, null, function(err, resp) {
          connection.release(err);
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
    self.getConnection(function(err, connection) {

      if (err) {
        callback(err);
        return;
      }

      connection.client.mutateRow(self._prefix + table, rowKey, columns, null, function(err, resp) {
        connection.release(err);
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
  self.getConnection(function(err, connection) {

    if (err) {
      callback(err);
      return;
    }

    connection.client.getRow(self._prefix + table, rowkey, null, function (err, rows) {
      var row = null;

      connection.release(err);
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
