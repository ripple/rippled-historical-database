var Promise    = require('bluebird');
var thrift     = require('thrift');
var moment     = require('moment');
var HBase      = require('./gen/Hbase');
var HBaseTypes = require('./gen/Hbase_types');
var Logger     = require('../../logger');
var Int64BE    = require('int64-buffer').Int64BE;

/**
 * HbaseClient
 * HBase client class
 */

var HbaseClient = function (options) {
  var self  = this;

  this.max_sockets = options.max_sockets || 1000;
  this._prefix     = options.prefix || '';
  this._servers    = options.servers || null;
  this._timeout    = options.timeout || 30000; //also acts as keepalive
  this._connection = null;
  this.hbase       = null;
  this.logStats    = (!options.logLevel || options.logLevel > 2) ? true : false;
  this.log         = new Logger({
    scope: 'hbase-thrift',
    level: options.logLevel,
    file: options.logFile
  });

  this.pool = [ ];

  if (!this._servers) {
    this._servers = [{
      host: options.host,
      port: options.port
    }];
  }

  // report the number of connections
  // every 60 seconds
  if (this.logStats) {
    setInterval(function() {
      self.log.info('connections:' + self.pool.length);
    }, 60 * 1000);
  }
};

/**
 * _getConnection
 * get an hbase connection from the pool
 */

HbaseClient.prototype._getConnection = function(callback) {
  var self = this;

  getOpenConnection(0, callback);

  function getOpenConnection(attempts, cb) {
    var i = self.pool.length;

    if (!attempts) {
      attempts = 0;

    } else if (attempts > 100) {
      cb('unable to get open connection');
      return;
    }


    //look for a free socket
    while (i--) {
      if (self.pool[i].client &&
          self.pool[i].connected &&
          Object.keys(self.pool[i].client._reqs).length < 10 &&
          !self.pool[i].keep) {

        cb(null, self.pool[i]);
        self.log.debug("# connections:", self.pool.length, ' - current:', i);
        return;
      }
    }

    //open a new socket if there is room in the pool
    if (self.pool.length < self.max_sockets) {
      openNewSocket(self.pool.length % self._servers.length);
    }

    //recheck for connected socket
    setTimeout(getOpenConnection.bind(self, attempts + 1, cb), 50);
  }

  /**
   * openNewSocket
   */

  function openNewSocket(i) {
    var server = self._servers[i || 0];

    //create new connection
    var connection = thrift.createConnection(server.host, server.port, {
      transport: thrift.TFramedTransport,
      protocol: thrift.TBinaryProtocol,
      timeout : self._timeout
    });


    //handle errors
    connection.error = function (err) {
      this.connected = false;

      //execute any callbacks, then delete
      if (this.client) {
        for (var key in this.client._reqs) {
          this.client._reqs[key](err);
          delete (this.client._reqs[key]);
        }
      }

      //destroy the connection
      this.connection.destroy();

      //remove from pool
      for (var i=0; i<self.pool.length; i++) {
        if (self.pool[i] === this) {
          delete self.pool[i];
          self.pool.splice(i, 1);
          break;
        }
      }
    };

    self.pool.push(connection);

    connection.on('timeout', function() {
      this.error('thrift client connection timeout');
    });

    connection.once('connect', function() {
      this.client = thrift.createClient(HBase, connection);
    });

    connection.on('error', function (err) {
      this.error('thrift connection error: ' + err);
    });

    connection.on('close', function() {
      this.error('hbase connection closed');
    });
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
    var swap;

    if (err) {
      self.log.error("unable to get connection", err);
      return;
    }

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

    if (options.batchSize) scanOpts.batchSize = options.batchSize;
    if (options.caching)   scanOpts.caching   = options.caching;
    if (options.columns)   scanOpts.columns = options.columns;

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

      connection.client.scannerGetList(scan_id, options.count, function (err, rows) {
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
        callback(null, results);
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

/*
 * buildSingleColumnValueFilters
 * helper to build column value filters
 */

HbaseClient.prototype.buildSingleColumnValueFilters = function (maybeFilters) {
  var filterString= maybeFilters.map( function(o) {
      if(o.value && o.qualifier) {
        var filterMissing = o.filterMissing === false ? false : true;
        var latest = o.latest === false ? false : true;

        return ["SingleColumnValueFilter ('",
                o.family, "', '",
                o.qualifier, "', ",
                o.comparator, ", 'binary:",
                o.value, "', ",
                filterMissing, ", ",
                latest, ")"].join('');
      }
  }).filter(function(n){ return n!=undefined }).join(' AND ');
  return filterString;
}

/*
 * markerWrapper
 * wrapper to add pagination logic (using the marker parameter)
 * marker may be encrypted at some point as exposing hbase keys to end-user may lead to DOS style attacks on our API
 */

/*
// How encryption could work
var crypto = require('crypto'),
  algorithm = 'aes-256-ecb',
  password = 'ripplepass';

function encrypt(text) {
  var cipher = crypto.createCipher(algorithm,password)
  var crypted = cipher.update(text,'utf8','hex')
  crypted += cipher.final('hex');
  return crypted;
}

function decrypt(text) {
  var decipher = crypto.createDecipher(algorithm,password)
  var dec = decipher.update(text,'hex','utf8')
  dec += decipher.final('utf8');
  return dec;
}
*/

// No encryption for now
function encrypt(text) {
  return text;
}

function decrypt(text) {
  return text;
}

function markerWrapper(f) {
  return function (scope, options, callback) {
    if(options.marker) {
      if(options.descending === true) {
        options.stopRow = decrypt(options.marker);
      } else {
        options.startRow = decrypt(options.marker);
      }
    }
    var limit = options.limit || 200;
    options.limit = +limit + 1;

    function callback1(err, res) {
      if(res) {
        var res1 = res.slice(0, res.length-1);
        var marker = res && res[+limit] ? encrypt(res[+limit].rowkey) : undefined;
        var fullres = {
          rows: res1,
          marker: marker
        };

        if(res1.length == +limit) {
          fullres = {
            rows: res1,
            marker: marker
          };

        } else {
          fullres = {
            rows: res
          };
        }
        callback(err, fullres);

      } else {
        callback(err, res);
      }
    }
    return f.apply(scope, [options, callback1]);
  };
}

/**
 * getScan
 */


HbaseClient.prototype.getScan = function (options, callback) {
  var self     = this;
  var prefix   = options.prefix || self._prefix;
  var table    = prefix + options.table;
  var scanOpts = { };
  var d        = Date.now();
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

    //invert stop and start index
    if (options.descending === true) {
      scanOpts.stopRow  = options.stopRow.toString();
      scanOpts.startRow = options.startRow.toString();
      scanOpts.reversed = true;

      if (scanOpts.startRow < scanOpts.stopRow) {
        swap              = scanOpts.startRow;
        scanOpts.startRow = scanOpts.stopRow;
        scanOpts.stopRow  = swap;
      }
    } else {
      scanOpts.startRow = options.stopRow.toString();
      scanOpts.stopRow  = options.startRow.toString();

      if (scanOpts.startRow > scanOpts.stopRow) {
        swap              = scanOpts.startRow;
        scanOpts.startRow = scanOpts.stopRow;
        scanOpts.stopRow  = swap;
      }
    }

    if (options.batchSize) scanOpts.batchSize = options.batchSize;
    if (options.caching)   scanOpts.caching   = options.caching;
    if (options.columns)   scanOpts.columns = options.columns;

    if (options.filterString && options.filterString !== '') {
      scanOpts.filterString = options.filterString;
    }

    scan = new HBaseTypes.TScan(scanOpts);

    connection.client.scannerOpenWithScan(table, scan, null, function(err, id) {

      if (err) {
        self.log.error(err);
        callback('unable to create scanner');
        connection.keep = false;
        return;
      }

      // get results from the scanner
      getResults(id, options.limit, function(err, rows) {

        // log stats
        if (self.logStats) {
          d = (Date.now() - d) / 1000;
          self.log.info('table:' + table + '.scan',
          'time:' + d + 's',
          rows ? 'rowcount:' + rows.length : '');
        }

        // close the scanner
        connection.client.scannerClose(id, function(err, resp) {
          if (err) {
            self.log.error('error closing scanner:', err);
          }
          // release
          connection.keep = false;
        });

        callback(err, rows);
      });
    });

    /**
     * getResults
     */

    function getResults(id, limit, callback) {
      var batchSize = 5000;
      var page = 1;
      var results = [];

      /**
       * recursiveGetResults
       */

      function recursiveGetResults() {
        var count;

        if (limit) {
          count = Math.min(batchSize, limit - (page - 1) * batchSize);
        } else {
          limit = Infinity;
          count = batchSize;
        }

        // get a batch
        connection.client.scannerGetList(id, count, function(err, rows) {
          if (rows && rows.length) {

            // add to the list
            results.push.apply(results, formatRows(rows, options.includeFamilies));

            // recursively get more
            // results if we hit the
            // count and are under the limit
            if (rows.length === count &&
                page * batchSize < limit) {
              page++;
              setImmediate(recursiveGetResults);
              return;
            }
          }

          callback(err, results);
        });
      }

      // recursively get results
      recursiveGetResults();
    }
  });
};

HbaseClient.prototype.getScanWithMarker = markerWrapper(HbaseClient.prototype.getScan);

/**
 * putRows
 * upload multiple rows for a single
 * table into HBase
 */

HbaseClient.prototype.putRows = function (options) {
  var self = this;
  var prefix = options.prefix || self._prefix;
  var table = prefix + options.table;
  var data = [];
  var arrays = [];
  var columns;
  var name;
  var value;
  var chunk = 100;


  //format rows
  for (rowKey in options.rows) {
    columns = self._prepareColumns(options.rows[rowKey]);

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
          reject(err);
          return;
        }

        connection.client.mutateRows(table, chunk, null, function(err, resp) {
          if (err) {
            self.log.error(table, err, resp);
            reject(err);

          } else {
            //self.log.info(table, "saved", chunk.length);
            resolve(data.length);
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

HbaseClient.prototype.putRow = function (options) {
  var self = this;
  var prefix = options.prefix || self._prefix;
  var table = prefix + options.table;
  var columns = self._prepareColumns(options.columns);


  function removeEmpty() {
    if (!options.removeEmptyColumns) {
      return Promise.resolve();
    }

    var removed = [];
    for (var key in options.columns) {
      if (!options.columns[key] && options.columns[key] !== 0) {
        removed.push(key);
      }
    }

    return self.deleteColumns({
      prefix: options.prefix,
      table: options.table,
      rowkey: options.rowkey,
      columns: removed
    });
  }

  function save() {
    return new Promise (function(resolve, reject) {
      self._getConnection(function(err, connection) {

        if (err) {
          reject(err);
          return;
        }

        connection.client.mutateRow(table, options.rowkey, columns, null, function(err, resp) {
          if (err) {
            self.log.error(table, err, resp);
            reject(err);

          } else {
            resolve(resp);
          }
        });
      });
    });
  }

  return removeEmpty()
  .then(save);
}

/**
 * increment
 */

HbaseClient.prototype.increment = function (options) {
  var self = this;
  //promisify
  return new Promise (function(resolve, reject) {
    self._getConnection(function(err, connection) {

      if (err) {
        callback(err);
        return;
      }

      var increment = new HBaseTypes.TIncrement({
        table: self._prefix + options.table,
        row: options.rowkey,
        column: 'inc:'+options.column,
        ammount: 1
      });

      connection.client.increment(increment, function(err, resp) {
        if (err) {
          self.log.error(self._prefix + options.table, err, resp);
          reject(err);

        } else {
          resolve(resp);
        }
      });
    });
  });
}

/**
 * deleteRow
 * delete a single row
 */

HbaseClient.prototype.deleteRow = function (options) {
  var self = this;
  var prefix = options.prefix || self._prefix;
  var table = prefix + options.table;

  //promisify
  return new Promise (function(resolve, reject) {
    self._getConnection(function(err, connection) {

      if (err) {
        callback(err);
        return;
      }

      connection.client.deleteAllRow(table, options.rowkey, null, function(err, resp) {
        if (err) {
          self.log.error(table, err, resp);
          reject(err);

        } else {
          resolve(resp);
        }
      });
    });
  });
}

/**
 * deleteRows
 * delete multiple rows
 * in a table
 */

HbaseClient.prototype.deleteRows = function (options) {
  var self = this;

  return Promise.map(options.rowkeys, function(rowkey) {
    return self.deleteRow({
      prefix: options.prefix,
      table: options.table,
      rowkey: rowkey
    });
  }).then(function(resp) {
    self.log.info(options.table, 'tables removed:', resp.length);
    return resp.length;
  });
}

/**
 * getRow
 */

HbaseClient.prototype.getRow = function(options, callback) {
  var self = this;
  var d = Date.now();
  var prefix = options.prefix || self._prefix;
  var table = prefix + options.table;

  function handleResponse(err, rows) {

    if (self.logStats) {
      d = (Date.now() - d) / 1000;
      self.log.info('table:' + table,
        'time:' + d + 's',
        rows ? 'rowcount:' + rows.length : '');
    }

    callback(err, rows ? formatRows(rows)[0] : undefined);
  }

  self._getConnection(function(err, connection) {

    if (err) {
      callback(err);
      return;
    }

    if (options.columns) {
      connection.client.getRowWithColumns(table,
                                          options.rowkey,
                                          options.columns,
                                          null,
                                          handleResponse);
    } else {
      connection.client.getRow(table,
                               options.rowkey,
                               null,
                               handleResponse);
    }
  });
};

HbaseClient.prototype.getRows = function(options, callback) {
  var self = this;
  var d = Date.now();
  var prefix = options.prefix || self._prefix;
  var table = prefix + options.table;

  function handleResponse(err, rows) {
    if (self.logStats) {
      d = (Date.now() - d) / 1000;
      self.log.info('table:' + table,
      'time:' + d + 's',
      rows ? 'rowcount:' + rows.length : '');
    }

    callback(err, rows ? formatRows(rows) : []);
  }

  self._getConnection(function(err, connection) {

    if (err) {
      callback(err);
      return;
    }

    if (options.columns) {
      connection.client.getRowsWithColumns(table,
                                           options.rowkeys,
                                           options.columns,
                                           null,
                                           handleResponse);
    } else {
      connection.client.getRows(table,
                                options.rowkeys,
                                null,
                                handleResponse);
    }
  });
};

/**
 * deleteColumns
 */

HbaseClient.prototype.deleteColumns = function(options) {
  var self = this;

  return Promise.map(options.columns, function(d) {
    return self.deleteColumn({
      prefix: options.prefix,
      table: options.table,
      rowkey: options.rowkey,
      column: d
    });
  });
}

/**
 * deleteColumn
 */

HbaseClient.prototype.deleteColumn = function(options) {
  var self = this;
  var prefix = options.prefix || self._prefix;
  var table = prefix + options.table;

  return new Promise(function(resolve, reject) {
    self._getConnection(function(err, connection) {
      if (err) {
        return reject(err);
      }

      connection.client.deleteAll(table, options.rowkey,
                                  options.column, null, function(err, resp) {
        if (err) {
          reject(err);
        } else {
          resolve(resp);
        }
      });
    });
  });
}

/**
 * getAllRows
 * get all rows in a table
 */

HbaseClient.prototype.getAllRows = function(options) {
  var self = this;

  return new Promise (function(resolve, reject) {
    self.getScan({
      prefix: options.prefix,
      table: options.table,
      startRow: ' ',
      stopRow: '~~'
    }, function(err, resp) {
      if (err) {
        reject(err);
      } else {
        resolve(resp);
      }
    });
  });
};

/**
 * deleteAllRows
 * delete all rows in a table
 */

HbaseClient.prototype.deleteAllRows = function(options) {
  var self = this;

  return self.getAllRows(options)
  .then(function(rows) {
    const rowkeys = rows.map(function(row) {
      return row.rowkey;
    });

    return self.deleteRows({
      table: options.table,
      rowkeys: rowkeys
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
    if (!value && value !== 0) {
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

var Int64BE = require("int64-buffer").Int64BE;

function formatRows(data, includeFamilies) {
  var rows = [];
  data.forEach(function(row) {
    r = {};
    r.rowkey = row.row.toString('utf8');
    for (key in row.columns) {
      if (includeFamilies) {
        r[key] = row.columns[key].value.toString('utf8');

      } else {
        parts = key.split(':');
        if (parts[0] === 'inc') {
          r[parts[1]] = Int64BE(row.columns[key].value).toString();
        } else {
          r[parts[1]] = row.columns[key].value.toString('utf8');
        }
      }
    }

    rows.push(r);
  });

  return rows;
}

module.exports = HbaseClient;
