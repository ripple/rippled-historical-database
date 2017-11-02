/* eslint prefer-spread:1 */

'use strict'

var Promise = require('bluebird')
var thrift = require('thrift')
var HBase = require('./gen/Hbase')
var HBaseTypes = require('./gen/Hbase_types')
var Logger = require('../../logger')
var int64BE = require('int64-buffer').Int64BE

/**
 * formatRows
 */

function formatRows(data, includeFamilies) {
  var rows = []
  var r
  var parts

  data.forEach(function(row) {
    r = {}
    r.rowkey = row.row.toString('utf8')
    for (var key in row.columns) {
      if (includeFamilies) {
        r[key] = row.columns[key].value.toString('utf8')

      } else {
        parts = key.split(':')
        if (parts[0] === 'inc') {
          r[parts[1]] = int64BE(row.columns[key].value).toString()
        } else {
          r[parts[1]] = row.columns[key].value.toString('utf8')
        }
      }
    }

    rows.push(r)
  })

  return rows
}


/**
 * HbaseClient
 * HBase client class
 */

function HbaseClient(options) {
  var self = this

  this.max_sockets = options.max_sockets || 1000
  this._prefix = options.prefix || ''
  this._servers = options.servers || null
  this._timeout = options.timeout || 30000
  this._connection = null
  this.hbase = null
  this.logStats = (!options.logLevel || options.logLevel > 2) ? true : false
  this.log = new Logger({
    scope: 'hbase-thrift',
    level: options.logLevel,
    file: options.logFile
  })

  this.pool = []

  if (!this._servers) {
    this._servers = [{
      host: options.host,
      port: options.port
    }]
  }

  // report the number of connections
  // every 60 seconds
  if (this.logStats) {
    setInterval(function() {
      self.log.info('connections:' + self.pool.length)
    }, 60 * 1000)
  }
}

/**
 * _getConnection
 * get an hbase connection from the pool
 */

HbaseClient.prototype._getConnection = function(cb) {
  var self = this
  var timer = setTimeout(function() {
    cb('unable to get open connection, ' +
      self.pool.length + ' of ' + self.max_sockets + ' in use')
  }, self._timeout)


  /**
   * handleNewConnectionError
   */

  function handleNewConnectionError(err) {
    this.error('error opening connection: ' + err)
    clearTimeout(timer)
    cb(err)
  }

  /**
   * onConnect
   */

  function onConnect() {
    this.removeListener('error', handleNewConnectionError)
    this.client = thrift.createClient(HBase, this)

    this.on('timeout', function() {
      this.error('thrift client connection timeout')
    })

    this.on('close', function() {
      this.error('hbase connection closed')
    })

    this.on('error', function(err) {
      this.error('thrift connection error: ' + err)
    })

    clearTimeout(timer)
    cb(null, this)
  }

  /**
   * openNewSocket
   */

  function openNewSocket(i) {
    var server = self._servers[i || 0]

    // create new connection
    var connection = thrift.createConnection(server.host, server.port, {
      transport: thrift.TFramedTransport,
      protocol: thrift.TCompactProtocol,
      timeout: self._timeout
    })


    // handle errors
    connection.error = function(err) {
      this.connected = false

      // execute any callbacks, then delete
      if (this.client) {
        for (var key in this.client._reqs) {
          this.client._reqs[key](err)
          delete (this.client._reqs[key])
        }
      }

      // destroy the connection
      this.connection.destroy()

      // remove from pool
      for (var j = 0; j < self.pool.length; j++) {
        if (self.pool[j] === this) {
          delete self.pool[j]
          self.pool.splice(j, 1)
          break
        }
      }
    }

    self.pool.push(connection)
    connection.once('error', handleNewConnectionError)
    connection.once('connect', onConnect)
  }

  /**
   * getConnection
   */

  function getConnection() {
    var i = self.pool.length

    // look for a free socket
    while (i--) {
      if (self.pool[i].client &&
          self.pool[i].connected &&
          Object.keys(self.pool[i].client._reqs).length < 10 &&
          !self.pool[i].keep) {

        clearTimeout(timer)
        cb(null, self.pool[i])
        self.log.debug('# connections:', self.pool.length, '- current:', i)
        return
      }
    }

    // open a new socket if there is room in the pool
    if (self.pool.length < self.max_sockets) {
      openNewSocket(self.pool.length % self._servers.length)
    } else {
      setTimeout(getConnection, 10)
    }
  }

  getConnection()
}

/**
 * iterator
 * iterate through a scan, one at a time
 */

HbaseClient.prototype.iterator = function(options) {
  var self = this
  var table = self._prefix + options.table
  var scanOpts = { }
  var scan
  var scan_id
  var error

  // create scan
  self._getConnection(function(err, connection) {
    var swap

    if (err) {
      self.log.error('unable to get connection', err)
      return
    }

    // default to reversed,
    // invert stop and start index
    if (options.descending === false) {
      scanOpts.startRow = options.stopRow.toString()
      scanOpts.stopRow = options.startRow.toString()

      if (scanOpts.startRow > scanOpts.stopRow) {
        swap = scanOpts.startRow
        scanOpts.startRow = scanOpts.stopRow
        scanOpts.stopRow = swap
      }

    } else {
      scanOpts.stopRow = options.stopRow.toString()
      scanOpts.startRow = options.startRow.toString()
      scanOpts.reversed = true

      if (scanOpts.startRow < scanOpts.stopRow) {
        swap = scanOpts.startRow
        scanOpts.startRow = scanOpts.stopRow
        scanOpts.stopRow = swap
      }
    }

    if (options.batchSize) {
      scanOpts.batchSize = options.batchSize
    }

    if (options.caching) {
      scanOpts.caching = options.caching
    }

    if (options.columns) {
      scanOpts.columns = options.columns
    }

    scan = new HBaseTypes.TScan(scanOpts)

    connection.client.scannerOpenWithScan(table, scan, null,
    function(err2, id) {

      if (err2) {
        self.log.error('unable to create scanner', err2)
        error = err2
      }

      scan_id = id
    })
  })

  self.getNext = function(callback) {

    if (!scan_id && !error) {
      setTimeout(function() {
        self.getNext(callback)
      }, 50)
      return
    }

    // get connection
    self._getConnection(function(err, connection) {

      if (err) {
        callback(err)
        return
      }

      connection.client.scannerGetList(scan_id, options.count,
      function(err2, rows) {
        var results = []

        if (err2) {
          callback(err2)
          return
        }

        // format as json
        results = formatRows(rows || [])
        callback(null, results)
      })
    })
  }

  self.close = function() {

    if (!scan_id) {
      return
    }

    // close the scanner
    self._getConnection(function(err, connection) {

      if (err) {
        self.log.error('connection error:', err)
        return
      }

      connection.client.scannerClose(scan_id, function(err2) {
        if (err) {
          self.log.error('error closing scanner:', err2)
        }
      })
    })
  }

  return this
}

/*
 * buildSingleColumnValueFilters
 * helper to build column value filters
 */

HbaseClient.prototype.buildSingleColumnValueFilters = function(maybeFilters) {
  var filterString = maybeFilters.map(function(o) {
    if (o.value && o.qualifier) {
      var filterMissing = o.filterMissing === false ? false : true
      var latest = o.latest === false ? false : true

      return [
        'SingleColumnValueFilter (\'',
        o.family, '\', \'',
        o.qualifier, '\', ',
        o.comparator, ', \'binary:',
        o.value, '\', ',
        filterMissing, ', ',
        latest, ')'
      ].join('')

    } else {
      return undefined
    }

  })
  .filter(function(n) {
    return n !== undefined
  })
  .join(' AND ')

  return filterString
}

/*
 * markerWrapper
 * wrapper to add pagination logic (using the marker parameter)
 * marker may be encrypted at some point as exposing hbase
 * keys to end-user may lead to DOS style attacks on our API
 */

/*
// How encryption could work
var crypto = require('crypto'),
  algorithm = 'aes-256-ecb',
  password = 'ripplepass'

function encrypt(text) {
  var cipher = crypto.createCipher(algorithm,password)
  var crypted = cipher.update(text,'utf8','hex')
  crypted += cipher.final('hex')
  return crypted
}

function decrypt(text) {
  var decipher = crypto.createDecipher(algorithm,password)
  var dec = decipher.update(text,'hex','utf8')
  dec += decipher.final('utf8')
  return dec
}
*/

// No encryption for now
function encrypt(text) {
  return text
}

function decrypt(text) {
  return text
}

function markerWrapper(f) {
  return function(scope, options, callback) {
    if (options.marker) {
      if (options.descending === true) {
        options.stopRow = decrypt(options.marker)
      } else {
        options.startRow = decrypt(options.marker)
      }
    }
    var limit = Number(options.limit || 200)
    options.limit = limit + 1

    function callback1(err, res) {
      if (res) {
        var res1 = res.slice(0, res.length - 1)
        var marker = res && res[limit] ? encrypt(res[limit].rowkey) : undefined
        var fullres = {
          rows: res1,
          marker: marker
        }

        if (res1.length === limit) {
          fullres = {
            rows: res1,
            marker: marker
          }

        } else {
          fullres = {
            rows: res
          }
        }
        callback(err, fullres)

      } else {
        callback(err, res)
      }
    }
    return f.apply(scope, [options, callback1])
  }
}

/**
 * getScan
 */


HbaseClient.prototype.getScan = function(options, callback) {
  var self = this
  var prefix = options.prefix || self._prefix
  var table = prefix + options.table
  var scanOpts = { }
  var d = Date.now()
  var scan
  var swap

  // get connection
  self._getConnection(function(err, connection) {

    /**
     * getResults
     */

    function getResults(id, l, cb) {
      var results = []
      var limit = l

      /**
       * recursiveGetResults
       */

      function recursiveGetResults() {
        var count

        if (limit) {
          count = (limit / 5) > 5 ? Math.floor(limit / 5) : 5
         } else {
          limit = Infinity
          count = 500
        }

        // get a batch
        connection.client.scannerGetList(id, count, function(err2, rows) {
          var max = limit - results.length

          if (rows && rows.length) {

            if (rows.length > max) {
              rows = rows.slice(0, max)
            }

            results.push(...formatRows(rows, options.includeFamilies))


            // recursively get more
            // results if we hit the
            // count and are under the limit
            if (results.length < limit) {
              setImmediate(recursiveGetResults)
              return
            }
          }

          cb(err2, results)
        })
      }

      // recursively get results
      recursiveGetResults()
    }

    if (err) {
      callback(err)
      return
    }

    // keep till we are finished
    connection.keep = true

    // invert stop and start index
    if (options.descending === true) {
      scanOpts.stopRow = options.stopRow.toString()
      scanOpts.startRow = options.startRow.toString()
      scanOpts.reversed = true

      if (scanOpts.startRow < scanOpts.stopRow) {
        swap = scanOpts.startRow
        scanOpts.startRow = scanOpts.stopRow
        scanOpts.stopRow = swap
      }
    } else {
      scanOpts.startRow = options.stopRow.toString()
      scanOpts.stopRow = options.startRow.toString()

      if (scanOpts.startRow > scanOpts.stopRow) {
        swap = scanOpts.startRow
        scanOpts.startRow = scanOpts.stopRow
        scanOpts.stopRow = swap
      }
    }

    if (options.batchSize) {
      scanOpts.batchSize = options.batchSize
    }

    if (options.caching) {
      scanOpts.caching = options.caching
    }

    if (options.columns) {
      scanOpts.columns = options.columns
    }

    if (options.filterString && options.filterString !== '') {
      scanOpts.filterString = options.filterString
    }

    scan = new HBaseTypes.TScan(scanOpts)

    connection.client.scannerOpenWithScan(table, scan, null,
    function(err2, id) {

      if (err2) {
        self.log.error(err2)
        callback('unable to create scanner')
        connection.keep = false
        return
      }

      // get results from the scanner
      getResults(id, options.limit, function(err3, rows) {

        // log stats
        if (self.logStats) {
          d = (Date.now() - d) / 1000
          self.log.info('table:' + table + '.scan',
          'time:' + d + 's',
          rows ? 'rowcount:' + rows.length : '',
          'scan ID:' + id)
        }

        // close the scanner
        connection.client.scannerClose(id, function(err4) {
          if (err4) {
            self.log.error('error closing scanner:', err4)
          }
          // release
          connection.keep = false
        })

        callback(err3, rows)
      })
    })
  })
}

HbaseClient.prototype.getScanWithMarker =
  markerWrapper(HbaseClient.prototype.getScan)

/**
 * putRows
 * upload multiple rows for a single
 * table into HBase
 */

HbaseClient.prototype.putRows = function(options) {
  var self = this
  var prefix = options.prefix || self._prefix
  var table = prefix + options.table
  var data = []
  var arrays = []
  var columns
  var chunkSize = 100


  // format rows
  for (var rowKey in options.rows) {
    columns = self._prepareColumns(options.rows[rowKey])

    if (!columns.length) {
      continue
    }

    data.push(new HBaseTypes.BatchMutation({
      row: rowKey,
      mutations: columns
    }))
  }

  // only send it if we have data
  if (!data.length) {
    return Promise.resolve()
  }

  // chunk data at no more than 100 rows
  for (var i = 0, j = data.length; i < j; i += chunkSize) {
    arrays.push(data.slice(i, i + chunkSize))
  }

  return Promise.map(arrays, function(chunk) {
    return new Promise(function(resolve, reject) {
      self.log.info(table, '- saving ' + chunk.length + ' rows')
      self._getConnection(function(err, connection) {

        if (err) {
          reject(err)
          return
        }

        connection.client.mutateRows(table, chunk, null, function(err2, resp) {
          if (err2) {
            self.log.error(table, err2, resp)
            reject(err2)

          } else {
            resolve(data.length)
          }
        })
      })
    })
  })
}

/**
 * putRow
 * save a single row
 */

HbaseClient.prototype.putRow = function(options) {
  var self = this
  var prefix = options.prefix || self._prefix
  var table = prefix + options.table
  var columns = self._prepareColumns(options.columns)


  function removeEmpty() {
    if (!options.removeEmptyColumns) {
      return Promise.resolve()
    }

    var removed = []
    for (var key in options.columns) {
      if (!options.columns[key] && options.columns[key] !== 0) {
        removed.push(key)
      }
    }

    return self.deleteColumns({
      prefix: options.prefix,
      table: options.table,
      rowkey: options.rowkey,
      columns: removed
    })
  }

  function save() {
    return new Promise(function(resolve, reject) {
      self._getConnection(function(err, connection) {

        if (err) {
          reject(err)
          return
        }

        connection.client.mutateRow(table, options.rowkey, columns, null,
        function(err2, resp) {
          if (err2) {
            self.log.error(table, err2, resp)
            reject(err2)

          } else {
            resolve(resp)
          }
        })
      })
    })
  }

  return removeEmpty()
  .then(save)
}

/**
 * increment
 */

HbaseClient.prototype.increment = function(options) {
  var self = this

  return new Promise(function(resolve, reject) {
    self._getConnection(function(err, connection) {

      if (err) {
        reject(err)
        return
      }

      var increment = new HBaseTypes.TIncrement({
        table: self._prefix + options.table,
        row: options.rowkey,
        column: 'inc:' + options.column,
        ammount: 1
      })

      connection.client.increment(increment, function(err2, resp) {
        if (err2) {
          self.log.error(self._prefix + options.table, err2, resp)
          reject(err2)

        } else {
          resolve(resp)
        }
      })
    })
  })
}

/**
 * deleteRow
 * delete a single row
 */

HbaseClient.prototype.deleteRow = function(options) {
  var self = this
  var prefix = options.prefix || self._prefix
  var table = prefix + options.table

  return new Promise(function(resolve, reject) {
    self._getConnection(function(err, connection) {

      if (err) {
        reject(err)
        return
      }

      connection.client.deleteAllRow(table, options.rowkey, null,
      function(err2, resp) {
        if (err2) {
          self.log.error(table, err2, resp)
          reject(err2)

        } else {
          resolve(resp)
        }
      })
    })
  })
}

/**
 * deleteRows
 * delete multiple rows
 * in a table
 */

HbaseClient.prototype.deleteRows = function(options) {
  var self = this

  return Promise.map(options.rowkeys, function(rowkey) {
    return self.deleteRow({
      prefix: options.prefix,
      table: options.table,
      rowkey: rowkey
    })
  }).then(function(resp) {
    self.log.info(options.table, 'tables removed:', resp.length)
    return resp.length
  })
}

/**
 * getRow
 */

HbaseClient.prototype.getRow = function(options, callback) {
  var self = this
  var d = Date.now()
  var prefix = options.prefix || self._prefix
  var table = prefix + options.table

  function handleResponse(err, rows) {

    if (self.logStats) {
      d = (Date.now() - d) / 1000
      self.log.info('table:' + table,
        'time:' + d + 's',
        rows ? 'rowcount:' + rows.length : '')
    }

    callback(err, rows ? formatRows(rows)[0] : undefined)
  }

  self._getConnection(function(err, connection) {

    if (err) {
      callback(err)
      return
    }

    if (options.columns) {
      connection.client.getRowWithColumns(table,
                                          options.rowkey,
                                          options.columns,
                                          null,
                                          handleResponse)
    } else {
      connection.client.getRow(table,
                               options.rowkey,
                               null,
                               handleResponse)
    }
  })
}

HbaseClient.prototype.getRows = function(options, callback) {
  var self = this
  var d = Date.now()
  var prefix = options.prefix || self._prefix
  var table = prefix + options.table

  function handleResponse(err, rows) {
    if (self.logStats) {
      d = (Date.now() - d) / 1000
      self.log.info('table:' + table,
      'time:' + d + 's',
      rows ? 'rowcount:' + rows.length : '')
    }

    callback(err, rows ? formatRows(rows) : [])
  }

  self._getConnection(function(err, connection) {

    if (err) {
      callback(err)
      return
    }

    if (options.columns) {
      connection.client.getRowsWithColumns(table,
                                           options.rowkeys,
                                           options.columns,
                                           null,
                                           handleResponse)
    } else {
      connection.client.getRows(table,
                                options.rowkeys,
                                null,
                                handleResponse)
    }
  })
}

/**
 * deleteColumns
 */

HbaseClient.prototype.deleteColumns = function(options) {
  var self = this

  return Promise.map(options.columns, function(d) {
    return self.deleteColumn({
      prefix: options.prefix,
      table: options.table,
      rowkey: options.rowkey,
      column: d
    })
  })
}

/**
 * deleteColumn
 */

HbaseClient.prototype.deleteColumn = function(options) {
  var self = this
  var prefix = options.prefix || self._prefix
  var table = prefix + options.table

  return new Promise(function(resolve, reject) {
    self._getConnection(function(err, connection) {
      if (err) {
        reject(err)

      } else {
        // default family to 'd' for data
        var name = options.column.split(':')
        var column = name[1] ? name[0] : 'd'
        column += ':' + (name[1] ? name[1] : name[0])

        connection.client.deleteAll(table, options.rowkey, column, null,
        function(err2, resp) {
          if (err2) {
            reject(err)
          } else {
            resolve(resp)
          }
        })
      }
    })
  })
}

/**
 * getAllRows
 * get all rows in a table
 */

HbaseClient.prototype.getAllRows = function(options) {
  var self = this

  return new Promise(function(resolve, reject) {
    self.getScan({
      prefix: options.prefix,
      table: options.table,
      startRow: ' ',
      stopRow: '~~'
    }, function(err, resp) {
      if (err) {
        reject(err)
      } else {
        resolve(resp)
      }
    })
  })
}

/**
 * deleteAllRows
 * delete all rows in a table
 */

HbaseClient.prototype.deleteAllRows = function(options) {
  var self = this

  return self.getAllRows(options)
  .then(function(rows) {
    const rowkeys = rows.map(function(row) {
      return row.rowkey
    })

    return self.deleteRows({
      table: options.table,
      rowkeys: rowkeys
    })
  })
}

/**
 * prepareColumns
 * create an array of columnValue
 * objects for the given data
 */

HbaseClient.prototype._prepareColumns = function(data) {
  var columns = []
  var column
  var value

  /**
   * prepareColumn
   * create a columnValue object
   * for the given column
   */

  function prepareColumn(key, d) {
    var v = d
    var name
    var c

    // stringify JSON and arrays
    if (typeof v !== 'string') {
      v = JSON.stringify(v)
    }

    // default family to 'd' for data
    name = key.split(':')
    c = name[1] ? name[0] : 'd'
    c += ':' + (name[1] ? name[1] : name[0])

    return new HBaseTypes.Mutation({
      column: c,
      value: v
    })
  }

  for (column in data) {
    value = data[column]

    // ignore empty rows
    if (!value && value !== 0) {
      continue
    }

    columns.push(prepareColumn(column, value))
  }

  return columns
}

module.exports = HbaseClient
