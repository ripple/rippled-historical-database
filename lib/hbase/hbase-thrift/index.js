const Hbase = require('ripple-hbase-client')
const Logger = require('../../logger')
function Client(options) {
  this.hbase = new Hbase(options)
  this.log = new Logger({
    scope: 'hbase-thrift',
    level: options.logLevel,
    file: options.logFile
  })
}

Client.prototype.getScan = function(options, callback) {
  this.hbase.getScan(options)
  .then(resp => {
    const rows = []
    for (let i=0; i<resp.rows.length; i++) {
      resp.rows[i].columns.rowkey = resp.rows[i].rowkey
      rows.push(resp.rows[i].columns)
    }

    callback(null, rows)
  })
  .catch(callback)
}

Client.prototype.getScanWithMarker = function(_this, options, callback) {
  _this.hbase.getScan(options)
  .then(resp => {
    const rows = []
    for (let i=0; i<resp.rows.length; i++) {
      resp.rows[i].columns.rowkey = resp.rows[i].rowkey
      rows.push(resp.rows[i].columns)
    }

    callback(null, {
      rows: rows,
      marker: resp.marker
    })
  })
  .catch(callback)
}

Client.prototype.deleteAllRows = function(options) {
  var self = this;

  return self.hbase.getScan({
    table: options.table
  })
  .then(function(resp) {
    if (!resp || !resp.rows.length) {
      return
    }

    const rowkeys = resp.rows.map(function(row) {
      return row.rowkey;
    });

    return self.hbase.deleteRows({
      table: options.table,
      rowkeys: rowkeys
    })
  })
}

Client.prototype.getRow = function(options, callback) {
  this.hbase.getRow(options)
  .then(d => {
    const row = d ? d.columns : undefined
    if (d) {
      row.rowkey = d.rowkey
    }

    callback(null, row)
  })
  .catch(callback)
}

Client.prototype.putRows = function(options) {
  return this.hbase.putRows(options)
  .then(resp => {
    return [resp]
  })
}

Client.prototype.putRow = function(options) {
  return this.hbase.putRow(options)
}

Client.prototype.getRows = function(options, callback) {
  this.hbase.getRows(options)
  .then(resp => {
    const rows = []
    for (let i=0; i<resp.length; i++) {
      resp[i].columns.rowkey = resp[i].rowkey
      rows.push(resp[i].columns)
    }

    callback(null, rows)
  })
  .catch(callback)
}

Client.prototype.getAllRows = function(options) {
  return this.hbase.getScan({
    prefix: options.prefix,
    table: options.table
  })
  .then(resp => {
    const rows = []
    for (let i=0; i<resp.rows.length; i++) {
      resp.rows[i].columns.rowkey = resp.rows[i].rowkey
      rows.push(resp.rows[i].columns)
    }

    return rows
  })
}

Client.prototype.deleteRow = function(options) {
  this.hbase.deleteRow(options)
}

module.exports = Client
