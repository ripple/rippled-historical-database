var Promise    = require('bluebird');
var thrift     = require('thrift');
var moment     = require('moment');
var HBase      = require('./gen/Hbase');
var HBaseTypes = require('./gen/Hbase_types');
var Logger     = require('../../logger');

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
    level: options.logLevel || 0,
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

HbaseClient.prototype._getConnection = function(cb) {
  var self = this;
  var connection;

  var i = self.pool.length;

  //look for a free socket
  while (i--) {
    if (self.pool[i].client &&
        self.pool[i].connected &&
        !Object.keys(self.pool[i].client._reqs).length &&
        !self.pool[i].keep) {

      cb(null, self.pool[i]);
      self.log.debug("# connections", self.pool.length);
      return;
    }
    /*
    console.log(!!self.pool[i].client,
                !!self.pool[i].connected,
                !!self.pool[i].keep)

    if (self.pool[i].client && Object.keys(self.pool[i].client._reqs).length) {
      count++;
    }
    */
  }

  //open a new socket if there is room in the pool
  if (self.pool.length < self.max_sockets) {
    openNewSocket(self.pool.length % self._servers.length);
  }

  //recheck for connected socket
  setTimeout(function() {
    self._getConnection(cb);
  }, 20);

  return;

  /**
   * openNewSocket
   */

  function openNewSocket(i) {
    var server = self._servers[i || 0];

    //create new connection
    connection = thrift.createConnection(server.host, server.port, {
      transport : thrift.TFramedTransport,
      protocol  : thrift.TBinaryProtocol,
      timeout   : self._timeout
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
        return ["SingleColumnValueFilter ('",o.family, "', '",
                o.qualifier, "', ", o.comparator, ", 'binary:",
                o.value, "')"].join('');
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
      options.stopRow= decrypt(options.marker);
    }
    var limit= options.limit || 200;
    options.limit= +limit + 1;

    function callback1(err, res) {
      if(res) {
        var res1= res.slice(0, res.length-1);
        var marker= res && res[+limit] ? encrypt(res[+limit].rowkey) : undefined;
        if(marker) {
          if(options.descending === false) {
            options.stopRow= marker;
          } else {
            options.startRow= marker;
          }
        }
        var fullres= { rows: res1, marker: marker };
        if(res1.length== +limit) {
          fullres= { rows: res1, marker: marker };
        } else {
          fullres= { rows: res };
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
  var table    = self._prefix + options.table;
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

    if(options.filterString && options.filterString!="") {
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

      getResults(id, options.limit, 1, function (err, rows) {
        callback(err, rows);

        if (self.logStats) {
          d = (Date.now() - d)/1000;
          self.log.info('table:' + table + '.scan',
          'time:'+ d + 's',
          rows ? 'rowcount:' + rows.length : '');
        }

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

    var getResultsBatchSize = 1000;
    function getResults (id, limit, page, callback) {
      var limit = limit || getResultsBatchSize;
      var count= Math.min(limit, limit-(page-1)*getResultsBatchSize);

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

HbaseClient.prototype.getScanWithMarker = markerWrapper(HbaseClient.prototype.getScan);

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

/**
 * deleteRow
 * delete a single row
 */

HbaseClient.prototype.deleteRow = function (table, rowKey) {
  var self = this;
  //promisify
  return new Promise (function(resolve, reject) {
    self._getConnection(function(err, connection) {

      if (err) {
        callback(err);
        return;
      }

      connection.client.deleteAllRow(self._prefix + table, rowKey, null, function(err, resp) {
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

/**
 * deleteRows
 * delete multiple rows
 * in a table
 */

HbaseClient.prototype.deleteRows = function (table, keys) {
  var self = this;
  return Promise.map(keys, function(key) {
    return self.deleteRow(table, key);
  }).then(function(resp) {
    self.log.info(table, 'tables removed:', resp.length);
    return resp.length;
  });
}

/**
 * getRow
 */

HbaseClient.prototype.getRow = function(options, callback) {
  var self = this;
  var d = Date.now();

  function handleResponse(err, rows) {

    if (self.logStats) {
      d = (Date.now() - d) / 1000;
      self.log.info('table:' + self._prefix + options.table,
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
      connection.client.getRowWithColumns(self._prefix + options.table,
                                          options.rowkey,
                                          options.columns,
                                          null,
                                          handleResponse);
    } else {
      connection.client.getRow(self._prefix + options.table,
                               options.rowkey,
                               null,
                               handleResponse);
    }
  });
};

HbaseClient.prototype.getRows = function(options, callback) {
  var self = this;
  var d = Date.now();

  function handleResponse(err, rows) {
    if (self.logStats) {
      d = (Date.now() - d) / 1000;
      self.log.info('table:' + self._prefix + options.table,
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
      connection.client.getRowsWithColumns(self._prefix + options.table,
                                           options.rowkeys,
                                           options.columns,
                                           null,
                                           handleResponse);
    } else {
      connection.client.getRows(self._prefix + options.table,
                                options.rowkeys,
                                null,
                                handleResponse);
    }
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
