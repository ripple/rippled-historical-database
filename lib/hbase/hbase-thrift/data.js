'use strict'

var Promise = require('bluebird')
var moment = require('moment')
var smoment = require('../../smoment')
var utils = require('../../utils')
var Parser = require('../../ledgerParser')
var binary = require('ripple-binary-codec')

var isoUTC = 'YYYY-MM-DDTHH:mm:ss[Z]'
var EPOCH_OFFSET = 946684800
var LI_PAD = 12
var I_PAD = 5
var S_PAD = 12

var exchangeIntervals = [
  '1minute',
  '5minute',
  '15minute',
  '30minute',
  '1hour',
  '2hour',
  '4hour',
  '1day',
  '3day',
  '7day',
  '1month',
  '1year'
]

var HbaseClient = {}

/**
 * getLastValidated
 */

HbaseClient.getLastValidated = function(callback) {
  this.getRow({
    table: 'control',
    rowkey: 'last_validated'
  }, callback)
}

/**
 * getStats
 */

HbaseClient.getStats = function(options, callback) {

  var interval = options.interval || 'day'
  var startRow = interval + '|' + options.start.hbaseFormatStartRow()
  var endRow = interval + '|' + options.end.hbaseFormatStopRow()
  var includeFamilies = options.metrics || options.family ? false : true
  var filterString

  if (options.family) {
    filterString = 'FamilyFilter (=, \'binary:' + options.family + '\')'
  }

  this.getScanWithMarker(this, {
    table: 'agg_stats',
    startRow: startRow,
    stopRow: endRow,
    limit: options.limit || Infinity,
    descending: options.descending,
    marker: options.marker,
    filterString: filterString,
    columns: options.metrics,
    includeFamilies: includeFamilies
  }, function(err, res) {


    if (res) {
      res.interval = interval

      // group by family
      if (includeFamilies) {
        res.rows.forEach(function(row, i) {
          var parts = row.rowkey.split('|')
          var stats = {
            date: utils.unformatTime(parts[1]).format(isoUTC),
            type: { },
            result: { },
            metric: { }
          }

          for (var key in row) {
            if (key === 'rowkey') {
              continue
            }

            parts = key.split(':')
            stats[parts[0]][parts[1]] = Number(row[key])
          }

          res.rows[i] = stats
        })

      } else {
        res.rows.forEach(function(row) {
          var parts = row.rowkey.split('|')
          delete row.rowkey

          for (var key in row) {
            row[key] = Number(row[key])
          }

          row.date = utils.unformatTime(parts[1]).format(isoUTC)
        })
      }
    }
    callback(err, res)
  })
}

/**
 * getStatsRow
 */

HbaseClient.getStatsRow = function(options) {

  var self = this
  var time = options.time || moment.utc()
  var interval = options.interval || 'day'
  var rowkey

  time.startOf(interval === 'week' ? 'isoWeek' : interval)
  rowkey = interval + '|' + utils.formatTime(time)

  return new Promise(function(resolve, reject) {

    function handleResponse(err, rows) {
      var parts
      var stats = {
        time: time.format(),
        interval: interval,
        type: { },
        result: { },
        metric: {
          accounts_created: 0,
          transaction_count: 0,
          ledger_count: 0,
          tx_per_ledger: 0.0,
          ledger_interval: 0.0
        }
      }

      if (err) {
        reject(err)

      } else if (!rows.length) {
        resolve(stats)

      } else {
        for (var key in rows[0].columns) {
          parts = key.split(':')
          stats[parts[0]][parts[1]] = Number(rows[0].columns[key].value)
        }

        resolve(stats)
      }
    }

    self._getConnection(function(err, connection) {

      if (err) {
        reject(err)
        return
      }

      connection.client.getRow(self._prefix + 'agg_stats',
                               rowkey,
                               null,
                               handleResponse)
    })
  })
}

/**
 * getPayments
 */

HbaseClient.getPayments = function(options, callback) {
  var filters = []
  var filterString
  var table
  var startRow
  var endRow

  function formatPayments(rows) {
    rows.forEach(function(row) {

      row.executed_time = Number(row.executed_time)
      row.ledger_index = Number(row.ledger_index)

      if (row.tx_index) {
        row.tx_index = Number(row.tx_index)
      }

      if (row.destination_balance_changes) {
        row.destination_balance_changes =
          JSON.parse(row.destination_balance_changes)
      }

      if (row.source_balance_changes) {
        row.source_balance_changes =
          JSON.parse(row.source_balance_changes)
      }

      if (row.destination_tag) {
        row.destination_tag = Number(row.destination_tag)
      }

      if (row.source_tag) {
        row.source_tag = Number(row.source_tag)
      }
    })

    return rows
  }

  if (options.interval) {
    table = 'agg_payments'
    startRow = options.interval +
      '|' + options.currency +
      '|' + (options.issuer || '') +
      '|' + options.start.hbaseFormatStartRow()
    endRow = options.interval +
      '|' + options.currency +
      '|' + (options.issuer || '') +
      '|' + options.end.hbaseFormatStopRow()

  } else {
    table = 'payments'
    startRow = options.start.hbaseFormatStartRow()
    endRow = options.end.hbaseFormatStopRow()

    if (options.currency) {
      table = 'payments_by_currency'
      startRow = options.currency + '|' +
        (options.issuer || '') + '|' +
        startRow
      endRow = options.currency + '|' +
        (options.issuer || '') + '|' +
        endRow
/*

      filters.push({
        qualifier: 'currency',
        value: options.currency,
        family: 'f', comparator: '='
      })
    }

    if (options.issuer) {
      filters.push({
        qualifier: 'issuer',
        value: options.issuer,
        family: 'f', comparator: '='
      })

    */
    }


    if (options.reduce) {
      options.columns = [
        'd:delivered_amount',
        'f:currency',
        'f:issuer'
      ]
    }
  }

  filterString = this.buildSingleColumnValueFilters(filters)

  this.getScanWithMarker(this, {
    table: table,
    startRow: startRow,
    stopRow: endRow,
    limit: options.limit || Infinity,
    descending: options.descending,
    marker: options.marker,
    filterString: filterString,
    columns: options.columns
  }, function(err, data) {
    var amount
    var res = data

    if (options.interval) {
      if (res && res.rows) {
        res.rows.forEach(function(row) {
          row.count = Number(row.count)
          row.amount = Number(row.amount)
          row.average = Number(row.average)
        })
      }

    } else if (options.reduce) {
      amount = 0

      if (res && res.rows) {
        res.rows.forEach(function(row) {
          amount += Number(row.delivered_amount)
        })

        res = {
          amount: amount,
          count: res.rows.length
        }

      } else {
        res = {
          amount: 0,
          count: 0
        }
      }

    } else if (res && res.rows) {
      res.rows = formatPayments(res.rows || [])
    }

    callback(err, res)
  })
}

/**
 * getAggregateAccountPayments
 */

HbaseClient.getAggregateAccountPayments = function(options) {
  var self = this
  var keys = []
  var start
  var end

  function formatRows(rows) {
    var results = { }
    var resp = []

    function Bucket(key) {
      this.receiving_counterparties = []
      this.sending_counterparties = []
      this.payments = []
      this.payments_sent = 0
      this.payments_received = 0
      this.high_value_sent = 0
      this.high_value_received = 0
      this.total_value_sent = 0
      this.total_value_received = 0
      this.total_value = 0

      if (key) {
        var parts = key.split('|')
        this.date = utils.unformatTime(parts[0]).format(isoUTC)
        this.account = parts[1]
      }

      return this
    }

    rows.forEach(function(row) {
      var key = row.rowkey

      row.sending_counterparties =
        JSON.parse(row.sending_counterparties || '[]')
      row.receiving_counterparties =
        JSON.parse(row.receiving_counterparties || '[]')
      row.payments = JSON.parse(row.payments || '[]')
      row.payments_sent = Number(row.payments_sent || 0)
      row.payments_received = Number(row.payments_received || 0)
      row.high_value_sent = Number(row.high_value_sent || 0)
      row.high_value_received = Number(row.high_value_received || 0)
      row.total_value_sent = Number(row.total_value_sent || 0)
      row.total_value_received = Number(row.total_value_received || 0)
      row.total_value = Number(row.total_value || 0)
      row.date = smoment(row.date).format()
      delete row.rowkey

      if (keys.length) {
        results[key] = row
      }
    })

    if (keys.length) {
      keys.forEach(function(key) {
        resp.push(results[key] || new Bucket(key))
      })

    } else {
      resp = rows
    }

    return resp
  }

  if (options.account) {

    if (options.date) {
      keys.push(options.date.hbaseFormatStartRow() + '|' + options.account)

    } else {
      start = moment(options.start.moment)
      end = moment(options.end.moment)

      while (end.diff(start) >= 0) {
        keys.push(utils.formatTime(start) + '|' + options.account)
        start.add(1, 'day')
      }
    }

    return new Promise(function(resolve, reject) {
      self.getRows({
        table: 'agg_account_payments',
        rowkeys: keys
      }, function(err, rows) {

        if (err) {
          reject(err)
          return
        }

        resolve({
          rows: formatRows(rows || [])
        })
      })
    })

  } else {
    return new Promise(function(resolve, reject) {

      self.getScanWithMarker(self, {
        table: 'agg_account_payments',
        startRow: options.start.hbaseFormatStartRow(),
        stopRow: options.end.hbaseFormatStopRow(),
        limit: options.limit,
        descending: false,
        marker: options.marker
      }, function(err, resp) {

        if (err) {
          reject(err)
          return
        }

        resp.rows = formatRows(resp.rows)
        resolve(resp)
      })
    })
  }
}


/**
 * getAccountPaymentChannels
 * query account escrows
 */

HbaseClient.getAccountPaymentChannels = function(options, callback) {
  var table = 'account_payment_channels'
  var startRow = options.account + '|' + options.start.hbaseFormatStartRow()
  var endRow = options.account + '|' + options.end.hbaseFormatStopRow()

  function formatPaychan(rows) {
    rows.forEach(function(row) {
      var key = row.rowkey.split('|')

      row.ledger_index = Number(row.ledger_index)
      row.tx_index = Number(row.tx_index || key[3])

      if (row.destination_tag) {
        row.destination_tag = Number(row.destination_tag)
      }

      if (row.source_tag) {
        row.source_tag = Number(row.source_tag)
      }
    })

    return rows
  }

  var maybeFilters = [
    {
      qualifier: 'tx_type',
      value: options.type,
      family: 'f',
      comparator: '='
    }, {
      qualifier: 'destination',
      value: options.destination,
      family: 'f',
      comparator: '='
    }, {
      qualifier: 'destination_tag',
      value: options.destination_tag,
      family: 'f',
      comparator: '='
    }, {
      qualifier: 'source_tag',
      value: options.source_tag,
      family: 'f',
      comparator: '='
    }
  ]

  var filterString = this.buildSingleColumnValueFilters(maybeFilters)

  this.getScanWithMarker(this, {
    table: table,
    startRow: startRow,
    stopRow: endRow,
    limit: options.limit,
    descending: options.descending,
    marker: options.marker,
    filterString: filterString
  }, function(err, res) {
    if (res) {
      res.rows = formatPaychan(res.rows || [])
    }

    callback(err, res)
  })
}


/**
 * getAccountEscrows
 * query account escrows
 */

HbaseClient.getAccountEscrows = function(options, callback) {
  var table = 'account_escrows'
  var startRow = options.account + '|' + options.start.hbaseFormatStartRow()
  var endRow = options.account + '|' + options.end.hbaseFormatStopRow()

  function formatEscrows(rows) {
    rows.forEach(function(row) {
      var key = row.rowkey.split('|')

      row.ledger_index = Number(row.ledger_index)
      row.tx_index = Number(row.tx_index || key[3])

      if (row.create_tx_seq) {
        row.create_tx_seq = Number(row.create_tx_seq)
      }

      if (row.destination_tag) {
        row.destination_tag = Number(row.destination_tag)
      }

      if (row.source_tag) {
        row.source_tag = Number(row.source_tag)
      }
    })

    return rows
  }

  var maybeFilters = [
    {
      qualifier: 'tx_type',
      value: options.type,
      family: 'f',
      comparator: '='
    }, {
      qualifier: 'destination',
      value: options.destination,
      family: 'f',
      comparator: '='
    }, {
      qualifier: 'destination_tag',
      value: options.destination_tag,
      family: 'f',
      comparator: '='
    }, {
      qualifier: 'source_tag',
      value: options.source_tag,
      family: 'f',
      comparator: '='
    }
  ]

  var filterString = this.buildSingleColumnValueFilters(maybeFilters)

  this.getScanWithMarker(this, {
    table: table,
    startRow: startRow,
    stopRow: endRow,
    limit: options.limit,
    descending: options.descending,
    marker: options.marker,
    filterString: filterString
  }, function(err, res) {
    if (res) {
      res.rows = formatEscrows(res.rows || [])
    }

    callback(err, res)
  })
}

/**
 * getAccountPayments
 * query account payments
 */

HbaseClient.getAccountPayments = function(options, callback) {
  var table = 'account_payments'
  var startRow = options.account + '|' + options.start.hbaseFormatStartRow()
  var endRow = options.account + '|' + options.end.hbaseFormatStopRow()
  var type

  function formatPayments(rows) {
    rows.forEach(function(row) {
      var key = row.rowkey.split('|')

      row.executed_time = Number(row.executed_time)
      row.ledger_index = Number(row.ledger_index)
      row.tx_index = Number(row.tx_index || key[3])

      if (row.destination_balance_changes) {
        row.destination_balance_changes =
          JSON.parse(row.destination_balance_changes)
      }
      if (row.source_balance_changes) {
        row.source_balance_changes =
          JSON.parse(row.source_balance_changes)
      }

      if (row.destination_tag) {
        row.destination_tag = Number(row.destination_tag)
      }

      if (row.source_tag) {
        row.source_tag = Number(row.source_tag)
      }
    })

    return rows
  }

  if (options.currency) {
    options.currency = options.currency.toUpperCase()
  }

  if (options.type) {
    if (options.type === 'sent') {
      type = 'source'
    } else if (options.type === 'received') {
      type = 'destination'
    }
  }

  var maybeFilters = [
    {
      qualifier: 'currency',
      value: options.currency,
      family: 'f',
      comparator: '='
    }, {
      qualifier: type,
      value: options.account,
      family: 'f',
      comparator: '='
    }, {
      qualifier: 'destination_tag',
      value: options.destination_tag,
      family: 'f',
      comparator: '='
    }, {
      qualifier: 'source_tag',
      value: options.source_tag,
      family: 'f',
      comparator: '='
    }
  ]

  var filterString = this.buildSingleColumnValueFilters(maybeFilters)

  this.getScanWithMarker(this, {
    table: table,
    startRow: startRow,
    stopRow: endRow,
    limit: options.limit,
    descending: options.descending,
    marker: options.marker,
    filterString: filterString
  }, function(err, res) {
    if (res) {
      res.rows = formatPayments(res.rows || [])
    }

    callback(err, res)
  })
}

/**
 * getAccountStats
 */

HbaseClient.getAccountStats = function(options, callback) {
  var table
  var columns
  var startRow = options.account + '|' +
    options.start.hbaseFormatStartRow()
  var stopRow = options.account + '|' +
    options.end.hbaseFormatStartRow()

  function formatData(d) {
    if (/^\d+$/.test(d)) {
      return Number(d)
    } else {
      return d
    }
  }

  function formatResults(rows) {
    var results = []
    rows.forEach(function(row) {
      var d = {
        date: smoment(row['d:date']).format()
      }

      delete row.rowkey
      delete row['d:date']
      delete row['d:account']

      for (var key in row) {
        var parts = key.split(':')


        if (parts[0] === 'd' || parts[0] === 'f') {
          d[parts[1]] = formatData(row[key])

        } else {
          if (!d[parts[0]]) {
            d[parts[0]] = {}
          }

          d[parts[0]][parts[1]] = formatData(row[key])
        }
      }

      results.push(d)
    })

    return results
  }

  switch (options.family) {
    case 'value':
      table = 'agg_account_balance_changes'
      columns = [
        'd:date',
        'd:account_value',
        'd:balance_change_count'
      ]
      break
    case 'exchanges':
      table = 'agg_account_exchange'
      break
    default:
      table = 'agg_account_stats'
  }

  this.getScanWithMarker(this, {
    table: table,
    startRow: startRow,
    stopRow: stopRow,
    limit: options.limit,
    descending: options.descending,
    marker: options.marker,
    columns: columns,
    includeFamilies: true

  }, function(err, res) {

    if (res && res.rows) {
      res.rows = formatResults(res.rows)
    }

    callback(err, res)
  })
}

/**
 * getMetric
 */

HbaseClient.getMetric = function(options, callback) {
  var self = this
  var table = 'agg_metrics'
  var keyBase = options.metric
  var rowkey

  /**
   * get rates
   */

  function getRates(params, cb) {
    self.getExchanges(params, function(err, resp) {
      if (err || !resp) {
        cb(err)
        return
      }

      var rates = { }
      resp.rows.forEach(function(row) {
        rates[moment.utc(row.start).format()] = row.vwap
      })

      cb(null, rates)
    })
  }

  /**
   * handleResponse
   */

  function handleResponse(resp, rates) {
    resp.rows.forEach(function(row) {
      var time

      delete row.rowkey
      row.components = JSON.parse(row.components)
      row.exchange = JSON.parse(row.exchange)
      row.total = parseFloat(row.total || 0)
      row.count = Number(row.count || 0)
      row.exchangeRate = parseFloat(row.exchangeRate || 0)

      if (rates) {
        time = moment.utc(row.startTime || row.time).format()
        row.exchangeRate = rates[time] || 0
        row.exchange = options.exchange
        row.total *= row.exchangeRate

        row.components.forEach(function(c) {
          c.rate = row.exchangeRate ? c.rate / row.exchangeRate : 0

          if (c.convertedAmount) {
            c.converted_amount = c.convertedAmount
            delete c.convertedAmount
          }

          c.converted_amount *= row.exchangeRate
        })
      }

      if (options.metric === 'issued_value') {
        delete row.count
      }
    })

    callback(null, resp)
  }

  if (options.live) {
    rowkey = keyBase + '|live' +
      (options.live === 'day' ? '' : '|' + options.live)

    self.getRow({
      table: table,
      rowkey: rowkey
    }, function(err, data) {
      if (err) {
        callback(err)

      } else if (!data) {
        callback(null, {rows: []})

      } else if (options.exchange && data) {

        var params = {
          base: {currency: 'XRP'},
          counter: options.exchange,
          date: smoment(),
          live: true
        }

        self.getExchangeRate(params)
        .nodeify(function(e, rate) {
          var rates = { }
          var time = moment.utc(data.startTime || data.time).format()
          if (e) {
            self.log.error(e)
            callback('unable to determine exchange rate')
            return
          }

          rates[time] = rate
          handleResponse({rows: [data]}, rates)
        })
      } else {
        handleResponse({rows: [data]})
      }
    })

  } else {
    if (options.interval) {
      keyBase += '|' + options.interval
    } else if (options.metric !== 'issued_value') {
      keyBase += '|day'
    }

    self.getScanWithMarker(this, {
      table: 'agg_metrics',
      startRow: keyBase + '|' + options.start.hbaseFormatStartRow(),
      stopRow: keyBase + '|' + options.end.hbaseFormatStopRow(),
      descending: options.descending,
      limit: options.limit,
      marker: options.marker

    }, function(err, resp) {

      if (err || !resp || !resp.rows) {
        callback(err || 'server error')

      } else if (options.exchange && resp.rows.length) {
        getRates({
          base: {currency: 'XRP'},
          counter: options.exchange,
          start: options.start,
          end: options.end,
          limit: options.limit,
          interval: options.interval === 'week' ?
            '7day' : '1' + (options.interval || 'day')
        }, function(e, rates) {
          if (e) {
            self.log.error(e)
            callback('unable to determine exchange rate')
            return
          }

          handleResponse(resp, rates)
        })
      } else {
        handleResponse(resp)
      }
    })
  }
}


/**
 * getCapitalization
 */

HbaseClient.getCapitalization = function(options, callback) {
  var base = options.currency + '|' + options.issuer
  var format = 'YYYYMMDD'
  var keys = []
  var column
  var start
  var end

  function handleResponse(err, resp) {
    if (resp && resp.rows) {
      resp.rows.forEach(function(row, i) {
        var parts = row.rowkey.split('|')
        var amount = Number(row[column])

        // don't allow less than 0
        if (amount < 0) {
          amount = 0
        }

        resp.rows[i] = {
          date: utils.unformatTime(parts[2])
            .add(1, 'day')
            .format(isoUTC),
          amount: amount
        }
      })
    }

    callback(err, resp)
  }

  if (options.adjustedChanges) {
    column = 'hotwallet_adj_balancesowed'
  } else if (options.changes) {
    column = 'issuer_balance_changes'
  } else if (options.adjusted) {
    column = 'cummulative_hotwallet_adj_balancesowed'
  } else {
    column = 'cummulative_issuer_balance_changes'
  }

  if (options.interval && options.interval !== 'day') {
    if (options.interval === 'week') {
      start = smoment(options.start)
      start.moment.startOf('isoWeek')
      start.granularity = 'day'

      while (start.moment.diff(options.end.moment) <= 0) {
        start.moment.subtract(1, 'day')
        keys.push(base + '|' + start.format(format))
        start.moment.add(1, 'day').add(1, 'week')
      }

    } else if (options.interval === 'month') {
      start = smoment(options.start)
      start.moment.startOf('month')
      start.granularity = 'day'

      while (start.moment.diff(options.end.moment) <= 0) {
        start.moment.subtract(1, 'day')
        keys.push(base + '|' + start.format(format))
        start.moment.add(1, 'day').add(1, 'month')
      }

    } else {
      callback('invalid interval - use: day, week, month')
      return
    }

    this.getRows({
      table: 'issuer_balance_snapshot',
      rowkeys: keys,
      columns: ['d:' + column]
    }, function(err, resp) {
      handleResponse(err, {rows: resp || []})
    })

  } else {
    start = smoment(options.start)
    start.moment.startOf('day')

    end = smoment(options.end)
    end.moment.startOf('day')

    start = base + '|' + start.format(format)
    end = base + '|' + end.format(format)

    this.getScanWithMarker(this, {
      table: 'issuer_balance_snapshot',
      startRow: start,
      stopRow: end,
      limit: options.limit || Infinity,
      descending: options.descending,
      marker: options.marker,
      columns: ['d:' + column]
    }, handleResponse)
  }
}

/**
 * getTopCurrencies
 */

HbaseClient.getTopCurrencies = function(options, callback) {
  options.table = 'top_currencies'
  this.getTop(options, callback)
}

/**
 * getTopMarkets
 */

HbaseClient.getTopMarkets = function(options, callback) {
  options.table = 'top_markets'
  this.getTop(options, callback)
}

/**
 * getTop
 */

HbaseClient.getTop = function(options, callback) {
  var self = this
  var start
  var end

  function formatResults(list) {
    var results = []
    list.forEach(function(d) {

      d.counterparty_count = Number(d.counterparty_count)

      delete d.rowkey
      delete d.rank
      delete d.date
      delete d.close_time_human
      delete d.counterparty_count
      results.push(d)
    })

    return results
  }

  function getTopHelper() {
    self.getScan({
      table: options.table,
      startRow: start.format('YYYYMMDD'),
      stopRow: end.format('YYYYMMDD'),
      limit: options.limit
    }, function(err, resp) {
      if (resp && resp.length) {
        callback(null, formatResults(resp))
      } else {
        callback(err, resp)
      }
    })
  }

  if (options.table !== 'top_markets' &&
     options.table !== 'top_currencies') {
    callback('invalid table')
    return
  }

  if (options.date) {
    start = smoment(options.date)
    end = smoment(start)
    end.moment.add(1, 'day')
    getTopHelper(options, start, end)

  // get the latest in the table
  } else {
    start = smoment()
    end = smoment(0)

    self.getScan({
      table: options.table,
      startRow: start.format('YYYYMMDD'),
      stopRow: end.format('YYYYMMDD'),
      descending: true,
      limit: 1
    }, function(err, resp) {
      if (err || !resp.length) {
        callback(err || 'no markets found')
      } else {
        start = smoment(resp[0].date || resp[0].close_time_human)
        end = smoment(start)
        end.moment.add(1, 'day')
        getTopHelper()
      }
    })
  }
}

/**
 * getAccountTransaction
 */

HbaseClient.getAccountTransaction = function(options, callback) {
  var self = this

  self.getRow({
    table: 'lu_account_transactions',
    rowkey: options.account + '|' + utils.padNumber(options.sequence, S_PAD)
  }, function(err, resp) {
    if (err) {
      callback(err)
    } else if (resp) {
      self.getTransaction({
        tx_hash: resp.tx_hash,
        binary: options.binary
      }, callback)

    } else {
      callback(err, resp)
    }
  })
}

/**
 * getAccountTransactions
 */

HbaseClient.getAccountTransactions = function(options, callback) {
  var self = this
  var hashes = []
  var filters = []
  var table
  var startRow
  var stopRow

  if (options.minSequence || options.maxSequence) {
    table = 'lu_account_transactions'
    startRow = options.account + '|' +
      utils.padNumber(options.minSequence || 0, S_PAD)
    stopRow = options.account + '|' +
      utils.padNumber(options.maxSequence || 999999999999999, S_PAD)

  } else {
    table = 'lu_affected_account_transactions'
    startRow = options.account + '|' + options.start.hbaseFormatStartRow()
    stopRow = options.account + '|' + options.end.hbaseFormatStopRow()
  }

  if (options.type) {
    filters.push({
      qualifier: 'type',
      value: options.type,
      family: 'f',
      comparator: '='
    })
  }

  if (options.result) {
    filters.push({
      qualifier: 'result',
      value: options.result,
      family: 'f',
      comparator: '='
    })
  }

  self.getScanWithMarker(self, {
    table: table,
    startRow: startRow,
    stopRow: stopRow,
    descending: options.descending,
    limit: options.limit,
    marker: options.marker,
    filterString: self.buildSingleColumnValueFilters(filters)

  }, function(err, resp) {


    if (err) {
      callback(err)

    } else if (!resp.rows.length) {
      callback(null, [])

    } else {
      for (var i = 0; i < resp.rows.length; i++) {
        if (!resp.rows[i].tx_hash) {
          callback('missing tx hash for account: ' + options.account)
          return
        }

        hashes.push(resp.rows[i].tx_hash)
      }

      self.getTransactions({
        hashes: hashes,
        binary: options.binary,
        marker: resp.marker
      }, callback)
    }
  })
}

/**
 * getAccountBalanceChanges
 */

HbaseClient.getAccountBalanceChanges = function(options, callback) {
  var table = 'balance_changes'
  var startRow = options.account + '|' + options.start.hbaseFormatStartRow()
  var endRow = options.account + '|' + options.end.hbaseFormatStopRow()

  function formatChanges(rows) {
    rows.forEach(function(row) {
      row.tx_index = Number(row.tx_index)
      row.executed_time = Number(row.executed_time)
      row.ledger_index = Number(row.ledger_index)
      row.node_index = Number(row.node_index)

      if (row.node_index === -1) {
        delete row.node_index
      }
    })

    return rows
  }

  if (options.currency) {
    options.currency = options.currency.toUpperCase()
  }

  var maybeFilters = [
    {
      qualifier: 'currency',
      value: options.currency,
      family: 'f',
      comparator: '='
    }, {
      qualifier: 'counterparty',
      value: options.counterparty,
      family: 'f',
      comparator: '='
    }, {
      qualifier: 'change_type',
      value: options.type,
      family: 'f',
      comparator: '='
    }
  ]

  var filterString = this.buildSingleColumnValueFilters(maybeFilters)

  this.getScanWithMarker(this, {
    table: table,
    startRow: startRow,
    stopRow: endRow,
    limit: options.limit,
    marker: options.marker,
    descending: options.descending,
    filterString: filterString
  }, function(err, res) {
    if (res) {
      res.rows = formatChanges(res.rows || [])
    }

    callback(err, res)
  })
}

/**
 * getExchangeRate
 */

HbaseClient.getExchangeRate = function(options) {
  var self = this

  /**
   * getExchanges
   */

  function getExchanges(multiple, period, interval) {
    return new Promise(function(resolve, reject) {
      var start = smoment(options.date.format())
      start.moment.subtract(multiple, period)
      self.getExchanges({
        base: options.base,
        counter: options.counter,
        interval: interval,
        start: start,
        end: options.date,
        descending: false
      }, function(err, resp) {
        if (err) {
          reject(err)
        } else {
          var base = 0
          var counter = 0

          resp.rows.forEach(function(d) {
            base += d.base_volume
            counter += d.counter_volume
          })

          resolve(base ? counter / base : 0)
        }
      })
    })
  }

  // for rolling periods
  if (options.period) {
    options.date.moment.subtract(1, 'second')
    options.date.granularity = 'second'
    if (options.period === 'hour') {
      return getExchanges(1, 'hour', '5minute')

    } else if (options.period === 'day') {
      return getExchanges(1, 'day', '15minute')

    } else if (options.period === '3day') {
      return getExchanges(3, 'day', '1hour')

    } else if (options.period === '7day') {
      return getExchanges(7, 'day', '1hour')

    } else if (options.period === '30day') {
      return getExchanges(30, 'day', '1day')
    }
  }

  // get daily vwap rate
  function getDailyRate() {
    if (options.live) {
      return Promise.resolve()
    }

    return new Promise(function(resolve, reject) {
      var start = smoment(options.date.format())
      start.moment.startOf('day')
      self.getExchanges({
        base: options.base,
        counter: options.counter,
        interval: '1day',
        start: start,
        end: options.date,
        descending: false,
        limit: 1
      }, function(err, resp) {
        if (err) {
          reject(err)
        } else {
          resolve(resp.rows[0] ? Number(resp.rows[0].vwap || 0) : 0)
        }
      })
    })
  }

  // get last 50 trades within 2 weeks
  function getLatestRate() {
    return new Promise(function(resolve, reject) {
      var start = smoment(options.date.format())
      start.moment.subtract(14, 'days')
      self.getExchanges({
        base: options.base,
        counter: options.counter,
        start: start,
        end: options.date,
        descending: true,
        limit: 50,
        reduce: true
      }, function(err, resp) {
        if (err) {
          reject(err)
        } else if (resp.reduced) {
          if (resp.reduced.count >= 10 || !options.strict) {
            resolve(Number(resp.reduced.vwap || 0))
          } else {
            resolve(0)
          }
        }
      })
    })
  }

  if (!options.base) {
    options.base = {currency: 'XRP'}
  } if (!options.counter) {
    options.counter = {currency: 'XRP'}
  }

  // default to strict mode
  options.strict = options.strict === false ? false : true

  if (options.base.currency === options.counter.currency &&
      options.base.issuer === options.counter.issuer) {
    return Promise.resolve(1)

  } else {
    return Promise.all([
      getDailyRate(),
      getLatestRate()
    ])
    .then(function(rates) {
      if (rates[0] && rates[1]) {
        return Promise.resolve((rates[0] + rates[1]) / 2)
      } else {
        return Promise.resolve(rates[1])
      }
    })
  }
}

/**
 * getExchanges
 * query exchanges and
 * aggregated exchanges
 */

HbaseClient.getExchanges = function(options, callback) {
  var base = options.base.currency + '|' +
      (options.base.issuer || '')
  var counter = options.counter.currency + '|' +
      (options.counter.issuer || '')
  var table
  var keyBase
  var startRow
  var endRow
  var descending
  var columns

  /**
  * if the base/counter key was inverted, we need to swap
  * some of the values in the results
  */

  function invertPair(rows) {
    var swap
    var i

    if (options.unreduced) {

      for (i = 0; i < rows.length; i++) {
        rows[i].rate = 1 / rows[i].rate

        // swap base and counter vol
        swap = rows[i].base_amount
        rows[i].base_amount = rows[i].counter_amount
        rows[i].counter_amount = swap

        // swap buyer and seller
        swap = rows[i].buyer
        rows[i].buyer = rows[i].seller
        rows[i].seller = swap
      }

    } else {
      for (i = 0; i < rows.length; i++) {

        // swap base and counter vol
        swap = rows[i].base_volume
        rows[i].base_volume = rows[i].counter_volume
        rows[i].counter_volume = swap

        // swap high and low
        swap = 1 / rows[i].high
        rows[i].high = 1 / rows[i].low
        rows[i].low = swap

        // invert open, close, vwap
        rows[i].open = 1 / rows[i].open
        rows[i].close = 1 / rows[i].close
        rows[i].vwap = 1 / rows[i].vwap

        // invert buy_volume
        rows[i].buy_volume /= rows[i].vwap
      }
    }

    return rows
  }

  /**
   * formatExchanges
   */

  function formatExchanges(data) {
    var rows = data
    rows.forEach(function(row) {
      var key = row.rowkey.split('|')

      delete row.base_issuer
      delete row.base_currency
      delete row.counter_issuer
      delete row.counter_currency

      row.base_amount = parseFloat(row.base_amount)
      row.counter_amount = parseFloat(row.counter_amount)
      row.rate = parseFloat(row.rate)
      row.offer_sequence = Number(row.offer_sequence || 0)
      row.ledger_index = Number(row.ledger_index)
      row.tx_index = Number(key[6])
      row.node_index = Number(key[7])
      row.time = utils.unformatTime(key[4]).unix()
    })

    if (options.invert) {
      rows = invertPair(rows)
    }

    return rows
  }

  /**
   * formatAggregates
   */

  function formatAggregates(data) {
    var rows = data

    rows.forEach(function(row) {
      row.base_volume = parseFloat(row.base_volume)
      row.counter_volume = parseFloat(row.counter_volume)
      row.buy_volume = parseFloat(row.buy_volume)
      row.count = Number(row.count)
      row.open = parseFloat(row.open)
      row.high = parseFloat(row.high)
      row.low = parseFloat(row.low)
      row.close = parseFloat(row.close)
      row.vwap = parseFloat(row.vwap)
      row.close_time = Number(row.close_time)
      row.open_time = Number(row.open_time)
    })

    if (options.invert) {
      rows = invertPair(rows)
    }

    return rows
  }

  /**
   * reduce
   * reduce all rows
   */

  function reduce(data) {

    var rows
    var reduced = {
      open: 0,
      high: 0,
      low: Infinity,
      close: 0,
      base_volume: 0,
      counter_volume: 0,
      buy_volume: 0,
      count: 0,
      open_time: 0,
      close_time: 0,
      vwap: 0
    }

    rows = formatExchanges(data)

    // filter out small XRP amounts
    rows = rows.filter(function(row) {
      if (options.base.currency === 'XRP' &&
          row.base_amount < 0.0005) {
        return false
      } else if (options.counter.currency === 'XRP' &&
                 row.counter_amount < 0.0005) {
        return false
      } else {
        return true
      }
    })

    if (rows.length) {
      reduced.open_time = moment.unix(rows[0].time).utc().format()
      reduced.close_time = moment.unix(rows[rows.length - 1].time)
        .utc().format()

      reduced.open = rows[0].rate
      reduced.close = rows[rows.length - 1].rate
      reduced.count = rows.length

    } else {
      reduced.low = 0
      return reduced
    }

    rows.forEach(function(row) {
      reduced.base_volume += row.base_amount
      reduced.counter_volume += row.counter_amount

      if (row.rate < reduced.low) {
        reduced.low = row.rate
      }

      if (row.rate > reduced.high) {
        reduced.high = row.rate
      }

      if (row.buyer === row.taker) {
        reduced.buy_volume += row.base_amount
      }
    })

    reduced.vwap = reduced.counter_volume / reduced.base_volume
    return reduced
  }

  if (counter.toLowerCase() > base.toLowerCase()) {
    keyBase = base + '|' + counter

  } else {
    keyBase = counter + '|' + base
    options.invert = true
  }

  if (!options.interval) {
    table = 'exchanges'
    descending = options.descending ? true : false
    options.unreduced = true

    // only need certain columns
    if (options.reduce) {
      columns = [
        'd:base_amount',
        'd:counter_amount',
        'd:rate',
        'f:executed_time',
        'f:buyer',
        'f:seller',
        'f:taker'
      ]
    }

  } else if (exchangeIntervals.indexOf(options.interval) !== -1) {
    keyBase = options.interval + '|' + keyBase
    descending = options.descending ? true : false
    table = 'agg_exchanges'

  } else {
    callback('invalid interval: ' + options.interval)
    return
  }

  startRow = keyBase + '|' + options.start.hbaseFormatStartRow()
  endRow = keyBase + '|' + options.end.hbaseFormatStopRow()

  if (options.autobridged) {
    options.filterstring =
      'DependentColumnFilter(\'f\', \'autobridged_currency\')'
    if (columns) {
      columns.push('f:autobridged_currency')
    }
  }

  this.getScanWithMarker(this, {
    table: table,
    startRow: startRow,
    stopRow: endRow,
    marker: options.marker,
    limit: options.limit,
    descending: descending,
    columns: columns,
    filterString: options.filterstring
  }, function(err, resp) {
    var result

    if (resp &&
        options.reduce &&
        options.limit === 10000 &&
        resp.rows.length === 10000 &&
        resp.marker) {
      callback('too many rows')
      return

    } else if (resp) {
      result = resp
    } else {
      result = {
        rows: []
      }
    }

    if (!result.rows) {
      result.rows = []
    }

    if (options.reduce && options.unreduced) {
      if (descending) {
        result.rows.reverse()
      }

      result.reduced = reduce(result.rows)
    } else if (table === 'exchanges') {
      result.rows = formatExchanges(result.rows)
    } else {
      result.rows = formatAggregates(result.rows)
    }

    callback(err, resp)
  })
}

HbaseClient.getAccountExchanges = function(options, callback) {

  var maybeFilters = [
    {
      qualifier: 'base_currency',
      value: options.base.currency,
      family: 'f',
      comparator: '='
    }, {
      qualifier: 'base_issuer',
      value: options.base.issuer,
      family: 'f',
      comparator: '='
    }, {
      qualifier: 'counter_currency',
      value: options.counter.currency,
      family: 'f',
      comparator: '='
    }, {
      qualifier: 'counter_issuer',
      value: options.counter.issuer,
      family: 'f',
      comparator: '='
    }
  ]

  var filterString = this.buildSingleColumnValueFilters(maybeFilters)

  this.getScanWithMarker(this, {
    table: 'account_exchanges',
    startRow: options.account + '|' + options.start.hbaseFormatStartRow(),
    stopRow: options.account + '|' + options.end.hbaseFormatStopRow(),
    descending: options.descending,
    limit: options.limit,
    filterString: filterString,
    marker: options.marker
  }, function(err, res) {

    if (err) {
      callback(err)
      return
    }

    if (!res.rows) {
      res.rows = []
    }

    res.rows.forEach(function(row) {
      row.base_amount = parseFloat(row.base_amount)
      row.counter_amount = parseFloat(row.counter_amount)
      row.rate = parseFloat(row.rate)
      row.ledger_index = Number(row.ledger_index || 0)
      row.offer_sequence = Number(row.offer_sequence || 0)
      row.tx_index = Number(row.tx_index || 0)
      row.node_index = Number(row.node_index || 0)
    })

    callback(null, res)
  })
}

/**
 * getLedgersByIndex
 */

HbaseClient.getLedgersByIndex = function(options, callback) {
  this.getScan({
    table: 'lu_ledgers_by_index',
    startRow: utils.padNumber(Number(options.startIndex), LI_PAD),
    stopRow: utils.padNumber(Number(options.stopIndex) + 1, LI_PAD),
    descending: options.descending,
    limit: options.limit
  }, function(err, resp) {

    if (resp && resp.length) {
      resp.forEach(function(row, i) {
        var rowkey = row.rowkey.split('|')
        resp[i].ledger_index = parseInt(rowkey[0], 10)
        resp[i].close_time = parseInt(resp[i].close_time, 10)
      })
    }

    callback(err, resp)
  })
}

/**
 * getLedgersByTime
 */

HbaseClient.getLedgersByTime = function(options, callback) {
  this.getScan({
    table: 'lu_ledgers_by_time',
    startRow: smoment(options.start).hbaseFormatStartRow(),
    stopRow: smoment(options.end).hbaseFormatStopRow(),
    descending: options.descending,
    limit: options.limit
  }, callback)
}

/**
 * getLedger
 */

HbaseClient.getLedger = function(options, callback) {
  var self = this

  function getLedgerByHash(opts) {
    var hashes = []

    self.getRow({
      table: 'ledgers',
      rowkey: opts.ledger_hash
    }, function(err, ledger) {

      if (err || !ledger) {
        callback(err, null)
        return
      }

      delete ledger.rowkey

      if (ledger.parent_close_time) {
        ledger.parent_close_time = Number(ledger.parent_close_time)
        if (ledger.parent_close_time < EPOCH_OFFSET) {
          ledger.parent_close_time += EPOCH_OFFSET
        }
      }

      ledger.ledger_index = Number(ledger.ledger_index)
      ledger.close_time = Number(ledger.close_time)
      ledger.close_time_human = moment.unix(ledger.close_time).utc()
        .format('YYYY-MMM-DD HH:mm:ss')
      ledger.transactions = JSON.parse(ledger.transactions)

      // get transactions
      if (ledger.transactions.length &&
          (opts.expand || opts.binary)) {
        hashes = ledger.transactions
        ledger.transactions = []
        self.getTransactions({
          hashes: hashes,
          binary: opts.binary,
          include_ledger_hash: opts.include_ledger_hash

        }, function(e, resp) {

          if (e) {
            callback(e, null)
            return

          } else if (hashes.length !== resp.rows.length && !opts.invalid) {
            callback('missing transaction: ' +
                   resp.rows.length + ' of ' +
                   hashes.length + ' found')
            return
          }

          ledger.transactions = resp.rows
          callback(e, ledger)
        })

      // return the ledger as is
      } else if (opts.transactions) {
        callback(null, ledger)

      // remove tranactions array
      } else {
        delete ledger.transactions
        callback(null, ledger)
      }
    })
  }

  // get by hash
  if (options.ledger_hash) {
    getLedgerByHash(options)

  // get ledger by close time
  } else if (options.closeTime) {
    self.getLedgersByTime({
      start: moment.utc(0),
      end: options.closeTime,
      descending: true,
      limit: 1
    }, function(err, resp) {
      if (err || !resp || !resp.length) {
        callback(err, null)
        return
      }

      // use the ledger hash to get the ledger
      options.ledger_hash = resp[0].ledger_hash
      getLedgerByHash(options)
    })

  // get by index, or get latest
  } else {
    self.getLedgersByIndex({
      startIndex: options.ledger_index || 0,
      stopIndex: options.ledger_index || 999999999999,
      descending: options.ledger_index ? false : true,
      limit: options.pad || 2
    }, function(err, resp) {

      if (err || !resp || !resp.length) {
        callback(err, null)
        return

      //  submit error on duplicate ledger index
      } else if (resp.length > 1 && options.ledger_index) {
        callback('duplicate ledger index: ' + options.ledger_index, null)
        return

      // latest + padded leeway
      } else if (options.pad) {
        options.ledger_hash = resp[resp.length - 1].ledger_hash

      } else {
        options.ledger_hash = resp[0].ledger_hash
      }

      getLedgerByHash(options)
    })
  }
}

/**
 * getTransaction
 */

HbaseClient.getTransaction = function(options, callback) {
  options.hashes = [options.tx_hash]

  this.getTransactions(options, function(err, resp) {
    if (resp) {
      callback(null, resp.rows ? resp.rows[0] : undefined)
    } else {
      callback(err)
    }
  })
}

/**
 * getTransactions
 */

HbaseClient.getTransactions = function(options, callback) {
  var self = this

  function clone(d) {
    return JSON.parse(JSON.stringify(d))
  }

  function isPartialPayment(flags) {
    return 0x00020000 & flags
  }

  function compare(a, b) {
    if (Number(a.tx_index) < Number(b.tx_index)) {
      return -1
    } else {
      return 1
    }
  }

  function getTransactionsByTime(opts, cb) {
    var filters = []

    if (opts.type) {
      filters.push({
        qualifier: 'type',
        value: opts.type,
        family: 'f',
        comparator: '='
      })
    }

    if (opts.result) {
      filters.push({
        qualifier: 'result',
        value: opts.result,
        family: 'f',
        comparator: '='
      })
    }

    self.getScanWithMarker(self, {
      table: 'lu_transactions_by_time',
      startRow: opts.start.hbaseFormatStartRow(),
      stopRow: opts.end.hbaseFormatStopRow(),
      marker: opts.marker,
      descending: opts.descending,
      limit: opts.limit,
      filterString: self.buildSingleColumnValueFilters(filters),
      columns: ['d:tx_hash', 'f:type', 'f:result']
    }, function(err, resp) {

      if (resp) {
        resp.rows.forEach(function(row, i) {
          resp.rows[i] = row.tx_hash
        })
      }

      cb(err, resp)
    })
  }

  function getTransactionsFromHashes(opts, cb) {
    var results = {
      marker: opts.marker,
      rows: []
    }

    function formatTx(d) {
      var tx = { }

      tx.hash = d.rowkey
      tx.ledger_index = Number(d.ledger_index)
      tx.date = moment.unix(d.executed_time).utc()
      .format('YYYY-MM-DDTHH:mm:ssZ')

      if (opts.include_ledger_hash) {
        tx.ledger_hash = d.ledger_hash
      }

      if (opts.binary) {
        tx.tx = d.raw
        tx.meta = d.meta

      } else {
        tx.tx = binary.decode(d.raw)
        tx.meta = binary.decode(d.meta)

        // handle delivered_amount for successful payments
        if (tx.tx.TransactionType === 'Payment' &&
            tx.meta.TransactionResult === 'tesSUCCESS') {

          // DeliveredAmount is present
          if (tx.meta.DeliveredAmount) {
            tx.meta.delivered_amount = tx.meta.DeliveredAmount

          // not a partial payment
          } else if (!isPartialPayment(tx.tx.Flags)) {
            tx.meta.delivered_amount = clone(tx.tx.Amount)

          // partial payment without
          // DeliveredAmount after 4594094
          } else if (tx.ledger_index > 4594094) {
            tx.meta.delivered_amount = clone(tx.tx.Amount)

          // partial payment before 4594094
          } else {
            tx.meta.delivered_amount = 'unavailable'
          }
        }
      }

      return tx
    }

    self.getRows({
      table: 'transactions',
      rowkeys: opts.hashes,
      columns: [
        'f:executed_time',
        'f:ledger_index',
        'f:ledger_hash',
        'd:raw',
        'd:meta',
        'd:tx_index'
      ]
    }, function(err, resp) {

      if (err) {
        cb(err)
        return
      }

      if (resp) {

        if (opts.ledger) {
          resp.sort(compare)
        }


        try {
          results.rows = resp.map(formatTx)

        } catch (e) {
          cb(e)
          return
        }
      }

      cb(null, results)
    })
  }

  if (options.hashes) {
    getTransactionsFromHashes(options, callback)

  } else {
    getTransactionsByTime(options, function(err, resp) {

      if (err) {
        callback(err)

      } else if (resp && resp.rows) {
        options.marker = resp.marker // replace/add marker
        options.hashes = resp.rows
        getTransactionsFromHashes(options, callback)

      } else {
        callback(null, {rows: []})
      }
    })
  }
}

/**
 * getFeeStats
 */

HbaseClient.getFeeStats = function(options) {
  var self = this

  function formatRows(rows) {
    rows.forEach(function(r) {
      r.current_ledger_size = Number(r.current_ledger_size)
      r.current_queue_size = Number(r.current_queue_size)
      r.expected_ledger_size = Number(r.expected_ledger_size)
      r.minimum_fee = Number(r.minimum_fee)
      r.median_fee = Number(r.median_fee)
      r.open_ledger_fee = Number(r.open_ledger_fee)
      r.pct_max_queue_size = Number(r.pct_max_queue_size)
      delete r.rowkey
    })
  }

  if (options.interval) {
    var start = options.descending ?
        options.end.format() : options.start.format()
    var end = options.descending ?
        options.start.format() : options.end.format()
    var date = moment.utc(start).startOf(options.interval)
    var keys = []
    var max = (options.limit || 200) + 10 // get more than needed
    var d

    // use date from the marker
    if (options.marker) {
      date = moment.utc(options.marker.split('|')[1], 'YYYYMMDDHHmmss')

    // if not descending, and the start is
    // different than the date, there is some
    // time between this date and the provided
    // minumum, so advance to the next interval
    } else if (!options.descending && date.diff(start)) {
      date.add(1, options.interval)
    }

    if (options.descending) {
      while (date.diff(end) > 0) {
        d = smoment(date).hbaseFormatStartRow()
        keys.push(['raw', d].join('|'))
        date.add(-1, options.interval)
        if (keys.length === max) {
          break
        }
      }

    } else {
      while (date.diff(end) < 0) {
        d = smoment(date).hbaseFormatStartRow()
        keys.push(['raw', d].join('|'))
        date.add(1, options.interval)
        if (keys.length === max) {
          break
        }
      }
    }

    return new Promise(function(resolve, reject) {
      self.getRows({
        table: 'fee_stats',
        rowkeys: keys
      }, function(err, resp) {
        var marker
        var result

        if (err) {
          reject(err)
        } else {

          if (resp.length > options.limit) {
            marker = resp[options.limit].rowkey
            result = resp.splice(0, options.limit)
          } else {
            result = resp
          }

          formatRows(result)
          resolve({
            marker: marker,
            rows: result
          })
        }
      })
    })

  } else {
    return new Promise(function(resolve, reject) {
      var startRow = [
        'raw',
        options.start.hbaseFormatStartRow()
      ].join('|')

      var stopRow = [
        'raw',
        options.end.hbaseFormatStopRow()
      ].join('|')

      self.getScanWithMarker(self, {
        table: 'fee_stats',
        startRow: startRow,
        stopRow: stopRow,
        limit: options.limit,
        marker: options.marker,
        descending: options.descending
      }, function(err, resp) {
        if (err) {
          reject(err)
        } else {
          formatRows(resp.rows)
          resolve(resp)
        }
      })
    })
  }
}

/**
 * getNetworkFees
 */

HbaseClient.getNetworkFees = function(options) {
  var self = this
  var startRow = [
    options.interval,
    options.start.hbaseFormatStartRow()
  ].join('|')

  var stopRow = [
    options.interval,
    options.end.hbaseFormatStopRow()
  ].join('|')

  return new Promise(function(resolve, reject) {
    self.getScanWithMarker(self, {
      table: 'network_fees',
      startRow: startRow,
      stopRow: stopRow,
      limit: options.limit,
      marker: options.marker,
      descending: options.descending
    }, function(err, resp) {
      if (err) {
        reject(err)
      } else {
        resp.rows.forEach(function(r) {
          if (r.ledger_index) {
            r.ledger_index = Number(r.ledger_index)
          }

          r.avg = Number(r.avg)
          r.max = Number(r.max)
          r.min = Number(r.min)
          r.total = Number(r.total)
          r.tx_count = Number(r.tx_count)

          delete r.interval
          delete r.rowkey
        })

        resolve(resp)
      }
    })
  })
}

/**
 * getAccounts
 */

HbaseClient.getAccounts = function(options, callback) {
  var self = this
  var params

  /**
   * formatRows
   */

  function formatRows(resp) {
    var rows = []
    var parts

    for (var i = 0; i < resp.length; i++) {

      // aggregate rows
      if (options.interval) {
        parts = resp[i].rowkey.split('|')
        rows.push({
          date: utils.unformatTime(parts[1]).format(isoUTC),
          count: Number(resp[i].accounts_created)
        })

      // genesis ledger accounts
      } else if (resp[i].genesis_balance) {
        delete resp[i].rowkey
        rows.push({
          account: resp[i].account,
          executed_time: moment.unix(resp[i].executed_time),
          ledger_index: Number(resp[i].ledger_index),
          genesis_balance: resp[i].genesis_balance,
          genesis_index: Number(resp[i].genesis_index)
        })

      // single account rows
      } else {

        delete resp[i].rowkey
        delete resp[i].tx_index
        delete resp[i].client

        resp[i].ledger_index = Number(resp[i].ledger_index)
        resp[i].executed_time = moment.unix(resp[i].executed_time)
          .utc()
          .format()

        rows.push(resp[i])
      }
    }

    return rows
  }

  /**
   * getReducedAccounts
   */

  function getReducedAccounts() {
    var paramsList = []
    var filterString = 'FirstKeyOnlyFilter() AND KeyOnlyFilter()'
    var start
    var end

    start = moment.utc(options.start.format())
    end = moment.utc(options.end.format())

    if (Math.abs(end.diff(start, 'days')) > 31 && !options.parent) {

      // individual up to the first full week
      paramsList.push({
        table: 'accounts_created',
        startRow: utils.formatTime(start),
        stopRow: utils.formatTime(start.startOf('isoWeek').add(1, 'week')),
        descending: false,
        filterString: filterString
      })

      // individual from the last week to end
      paramsList.push({
        table: 'accounts_created',
        stopRow: utils.formatTime(end),
        startRow: utils.formatTime(end.startOf('isoWeek')),
        descending: false,
        filterString: filterString
      })

      // aggregate for the rest
      paramsList.push({
        table: 'agg_stats',
        startRow: 'week|' + utils.formatTime(start),
        stopRow: 'week|' + utils.formatTime(end),
        columns: ['metric:accounts_created'],
        descending: false
      })

    } else {
      if (options.parent) {
        filterString = self.buildSingleColumnValueFilters([{
          qualifier: 'parent',
          family: 'f',
          comparator: '=',
          value: options.parent
        }])
      }

      paramsList.push({
        table: 'accounts_created',
        startRow: utils.formatTime(start),
        stopRow: utils.formatTime(end),
        descending: false,
        filterString: filterString
      })
    }

    Promise.map(paramsList, function(p) {
      return new Promise(function(resolve, reject) {
        self.getScan(p, function(err, resp) {
          var count = 0
          if (err) {
            reject(err)

          } else if (p.table === 'accounts_created') {
            resolve(resp.length)

          } else {
            for (var i = 0; i < resp.length; i++) {
              count += Number(resp[i].accounts_created)
            }

            resolve(count)
          }
        })
      })
    }).nodeify(function(err, resp) {
      var total = 0
      var result

      if (resp) {
        resp.forEach(function(count) {
          total += count
        })

        result = {
          rows: [total]
        }
      }

      callback(err, result)
    })
  }

  // reduced to count
  if (options.reduce) {
    getReducedAccounts(options)
    return

  // counts over time
  } else if (options.interval) {
    params = {
      table: 'agg_stats',
      startRow: options.interval + '|' + options.start.hbaseFormatStartRow(),
      stopRow: options.interval + '|' + options.end.hbaseFormatStopRow(),
      columns: ['metric:accounts_created']
    }

  // individual rows
  } else {
    params = {
      table: 'accounts_created'
    }

    if (options.parent) {
      params.filterString = self.buildSingleColumnValueFilters([{
        qualifier: 'parent',
        family: 'f',
        comparator: '=',
        value: options.parent
      }])
    }

    if (options.account) {
      params.filterString = self.buildSingleColumnValueFilters([{
        qualifier: 'account',
        family: 'f',
        comparator: '=',
        value: options.account
      }])

      options.start = smoment(0)
      options.end = smoment()
    }

    params.startRow = options.start.hbaseFormatStartRow()
    params.stopRow = options.end.hbaseFormatStopRow()
  }

  params.limit = options.limit
  params.descending = options.descending
  params.marker = options.marker

  self.getScanWithMarker(this, params, function(err, resp) {
    if (resp && resp.rows) {
      resp.rows = formatRows(resp.rows)
    }

    callback(err, resp)
  })
}

/**
 * saveLedger
 */

HbaseClient.saveLedger = function(ledger, callback) {
  var self = this
  var tableNames = []
  var tables = self.prepareLedgerTables(ledger)

  tableNames = Object.keys(tables)

  Promise.map(tableNames, function(name) {
    return self.putRows({
      table: name,
      rows: tables[name]
    })
  })
  .nodeify(function(err, resp) {
    if (err) {
      self.log.error('error saving ledger:', ledger.ledger_index, err)
    } else {
      self.log.info('ledger saved:', ledger.ledger_index)
    }

    if (callback) {
      callback(err, resp)
    }
  })
}

/**
 * saveTransaction
 */

HbaseClient.saveTransaction = function(tx, callback) {
  this.saveTransactions([tx], callback)
}

/**
 * saveTransactions
 */

HbaseClient.saveTransactions = function(transactions, callback) {
  var self = this
  var tables = self.prepareTransactions(transactions)
  var tableNames = Object.keys(tables)

  Promise.map(tableNames, function(name) {
    return self.putRows({
      table: name,
      rows: tables[name]
    })
  })
  .nodeify(function(err) {
    if (err) {
      self.log.error('error saving transaction(s)', err)
    } else {
      self.log.info(transactions.length + ' transaction(s) saved')
    }

    if (callback) {
      callback(err, transactions.length)
    }
  })
}

/**
 * prepareLedgerTables
 */

HbaseClient.prepareLedgerTables = function(ledger) {
  var tables = {
    ledgers: { },
    lu_ledgers_by_index: { },
    lu_ledgers_by_time: { }
  }

  var ledgerIndexKey = utils.padNumber(ledger.ledger_index, LI_PAD) +
    '|' + ledger.ledger_hash

  var ledgerTimeKey = utils.formatTime(ledger.close_time) +
    '|' + utils.padNumber(ledger.ledger_index, LI_PAD)

  // add formated ledger
  tables.ledgers[ledger.ledger_hash] = ledger

  // add ledger index lookup
  tables.lu_ledgers_by_index[ledgerIndexKey] = {
    ledger_hash: ledger.ledger_hash,
    parent_hash: ledger.parent_hash,
    'f:ledger_index': ledger.ledger_index,
    'f:close_time': ledger.close_time
  }

  // add ledger by time lookup
  tables.lu_ledgers_by_time[ledgerTimeKey] = {
    ledger_hash: ledger.ledger_hash,
    parent_hash: ledger.parent_hash,
    'f:ledger_index': ledger.ledger_index,
    'f:close_time': ledger.close_time
  }

  return tables
}

/*
 * prepareTransactions
 */

HbaseClient.prepareTransactions = function(transactions) {
  var data = {
    transactions: { },
    lu_transactions_by_time: { },
    lu_account_transactions: { }
  }

  transactions.forEach(function(tx) {
    var key

    // transactions by time
    key = utils.formatTime(tx.executed_time) +
      '|' + utils.padNumber(tx.ledger_index, LI_PAD) +
      '|' + utils.padNumber(tx.tx_index, I_PAD)

    data.lu_transactions_by_time[key] = {
      tx_hash: tx.hash,
      tx_index: tx.tx_index,
      'f:executed_time': tx.executed_time,
      'f:ledger_index': tx.ledger_index,
      'f:type': tx.TransactionType,
      'f:result': tx.tx_result
    }

    // transactions by account sequence
    key = tx.Account + '|' + utils.padNumber(tx.Sequence, S_PAD)

    data.lu_account_transactions[key] = {
      tx_hash: tx.hash,
      sequence: tx.Sequence,
      'f:executed_time': tx.executed_time,
      'f:ledger_index': tx.ledger_index,
      'f:type': tx.TransactionType,
      'f:result': tx.tx_result
    }

    tx['f:Account'] = tx.Account
    tx['f:Sequence'] = tx.Sequence
    tx['f:tx_result'] = tx.tx_result
    tx['f:TransactionType'] = tx.TransactionType
    tx['f:executed_time'] = tx.executed_time
    tx['f:ledger_index'] = tx.ledger_index
    tx['f:ledger_hash'] = tx.ledger_hash
    tx['f:client'] = tx.client

    delete tx.Account
    delete tx.Sequence
    delete tx.tx_result
    delete tx.TransactionType
    delete tx.executed_time
    delete tx.ledger_index
    delete tx.ledger_hash
    delete tx.client

    // add transaction
    data.transactions[tx.hash] = tx
  })

  return data
}

/**
 * prepareParsedData
 */

HbaseClient.prepareParsedData = function(data) {
  var tables = {
    exchanges: { },
    account_offers: { },
    account_exchanges: { },
    balance_changes: { },
    payments: { },
    payments_by_currency: { },
    escrows: { },
    payment_channels: { },
    account_escrows: { },
    account_payments: { },
    accounts_created: { },
    account_payment_channels: { },
    memos: { },
    lu_account_memos: { },
    lu_affected_account_transactions: { },
    lu_account_offers_by_sequence: { }
  }

  // add exchanges
  data.exchanges.forEach(function(ex) {
    var suffix = utils.formatTime(ex.time) +
      '|' + utils.padNumber(ex.ledger_index, LI_PAD) +
      '|' + utils.padNumber(ex.tx_index, I_PAD) +
      '|' + utils.padNumber(ex.node_index, I_PAD) // guarantee uniqueness

    var key = ex.base.currency +
      '|' + (ex.base.issuer || '') +
      '|' + ex.counter.currency +
      '|' + (ex.counter.issuer || '') +
      '|' + suffix

    var key2 = ex.buyer + '|' + suffix
    var key3 = ex.seller + '|' + suffix
    var row = {
      'f:base_currency': ex.base.currency,
      'f:base_issuer': ex.base.issuer || undefined,
      base_amount: ex.base.amount,
      'f:counter_currency': ex.counter.currency,
      'f:counter_issuer': ex.counter.issuer || undefined,
      counter_amount: ex.counter.amount,
      rate: ex.rate,
      'f:buyer': ex.buyer,
      'f:seller': ex.seller,
      'f:taker': ex.taker,
      'f:provider': ex.provider,
      'f:offer_sequence': ex.sequence,
      'f:tx_hash': ex.tx_hash,
      'f:executed_time': ex.time,
      'f:ledger_index': ex.ledger_index,
      'f:tx_type': ex.tx_type,
      'f:client': ex.client,
      tx_index: ex.tx_index,
      node_index: ex.node_index
    }

    if (ex.autobridged) {
      row['f:autobridged_currency'] = ex.autobridged.currency
      row['f:autobridged_issuer'] = ex.autobridged.issuer
    }

    tables.exchanges[key] = row
    tables.account_exchanges[key2] = row
    tables.account_exchanges[key3] = row
  })

  // add offers
  data.offers.forEach(function(o) {

    var key = o.account +
      '|' + utils.formatTime(o.executed_time) +
      '|' + utils.padNumber(o.ledger_index, LI_PAD) +
      '|' + utils.padNumber(o.tx_index, I_PAD) +
      '|' + utils.padNumber(o.node_index, I_PAD)

    tables.account_offers[key] = {
      'f:tx_type': o.tx_type,
      'f:account': o.account,
      'f:offer_sequence': o.offer_sequence,
      'f:node_type': o.node_type,
      'f:change_type': o.change_type,
      'f:pays_currency': o.taker_pays.currency,
      'f:pays_issuer': o.taker_pays.issuer || undefined,
      pays_amount: o.taker_pays.value,
      pays_change: o.pays_change,
      'f:gets_currency': o.taker_gets.currency,
      'f:gets_issuer': o.taker_gets.issuer || undefined,
      gets_amount: o.taker_gets.value,
      gets_change: o.gets_change,
      rate: o.rate,
      'f:book_directory': o.book_directory,
      'f:expiration': o.expiration,
      'f:next_offer_sequence': o.next_offer_sequence,
      'f:prev_offer_sequence': o.prev_offer_sequence,
      'f:executed_time': o.executed_time,
      'f:ledger_index': o.ledger_index,
      'f:client': o.client,
      tx_index: o.tx_index,
      node_index: o.node_index,
      tx_hash: o.tx_hash
    }

    key = o.account +
      '|' + o.sequence +
      '|' + utils.padNumber(o.ledger_index, LI_PAD) +
      '|' + utils.padNumber(o.tx_index, I_PAD) +
      '|' + utils.padNumber(o.node_index, I_PAD)

    tables.lu_account_offers_by_sequence[o.account + '|' + o.sequence] = {
      'f:account': o.account,
      'f:sequence': o.sequence,
      'f:type': o.type,
      'f:executed_time': o.executed_time,
      'f:ledger_index': o.ledger_index,
      tx_index: o.tx_index,
      node_index: o.node_index,
      tx_hash: o.tx_hash
    }
  })

  // add balance changes
  data.balanceChanges.forEach(function(c) {
    var suffix = '|' + utils.formatTime(c.time) +
      '|' + utils.padNumber(c.ledger_index, LI_PAD) +
      '|' + utils.padNumber(c.tx_index, I_PAD) +
      '|' + (c.node_index === -1 ? '$' : utils.padNumber(c.node_index, I_PAD))

    var row = {
      'f:account': c.account,
      'f:counterparty': c.counterparty,
      'f:currency': c.currency,
      amount_change: c.change,
      final_balance: c.final_balance,
      'f:change_type': c.type,
      'f:tx_hash': c.tx_hash,
      'f:executed_time': c.time,
      'f:ledger_index': c.ledger_index,
      tx_index: c.tx_index,
      node_index: c.node_index,
      'f:client': c.client,
      'f:escrow_counterparty': c.escrow_counterparty,
      escrow_balance_change: c.escrow_balance_change,
      'f:paychannel_counterparty': c.paychannel_counterparty,
      paychannel_fund_change: c.paychannel_fund_change,
      paychannel_fund_final_balance: c.paychannel_fund_final_balance,
      paychannel_final_balance: c.paychannel_final_balance
    }

    tables.balance_changes[c.account + suffix] = row
  })

  data.payments.forEach(function(p) {
    var key = utils.formatTime(p.time) +
      '|' + utils.padNumber(p.ledger_index, LI_PAD) +
      '|' + utils.padNumber(p.tx_index, I_PAD)
    var currency = p.currency + '|' + (p.issuer || '')

    var payment = {
      'f:source': p.source,
      'f:destination': p.destination,
      amount: p.amount,
      delivered_amount: p.delivered_amount,
      'f:currency': p.currency,
      'f:issuer': p.issuer,
      'f:source_currency': p.source_currency,
      fee: p.fee,
      source_balance_changes: p.source_balance_changes,
      destination_balance_changes: p.destination_balance_changes,
      'f:tx_hash': p.tx_hash,
      'f:executed_time': p.time,
      'f:ledger_index': p.ledger_index,
      tx_index: p.tx_index,
      'f:client': p.client
    }

    if (p.max_amount) {
      payment.max_amount = p.max_amount
    }

    if (p.destination_tag) {
      payment['f:destination_tag'] = p.destination_tag
    }

    if (p.source_tag) {
      payment['f:source_tag'] = p.source_tag
    }

    if (p.invoice_id) {
      payment['f:invoice_id'] = p.invoice_id
    }

    tables.payments[key] = payment
    tables.payments_by_currency[currency + '|' + key] = payment
    tables.account_payments[p.source + '|' + key] = payment
    tables.account_payments[p.destination + '|' + key] = payment
  })

  // add escrows
  data.escrows.forEach(function(d) {
    var key = utils.formatTime(d.time) +
      '|' + utils.padNumber(d.ledger_index, LI_PAD) +
      '|' + utils.padNumber(d.tx_index, I_PAD)

    var escrow = {
      'f:tx_type': d.tx_type,
      'f:account': d.account,
      'f:owner': d.owner,
      'f:destination': d.destination,
      'f:destination_tag': d.destination_tag,
      'f:source_tag': d.source_tag,
      create_tx: d.create_tx,
      create_tx_seq: d.create_tx_seq,
      condition: d.condition,
      fulfillment: d.fulfillment,
      amount: d.amount,
      flags: d.flags,
      fee: d.fee,
      'f:tx_hash': d.tx_hash,
      'f:executed_time': d.time,
      'f:cancel_after': d.cancel_after,
      'f:finish_after': d.finish_after,
      'f:ledger_index': d.ledger_index,
      tx_index: d.tx_index,
      'f:client': d.client
    }

    tables.escrows[key] = escrow
    tables.account_escrows[d.owner + '|' + key] = escrow
    tables.account_escrows[d.destination + '|' + key] = escrow
  })

  // add paychan
  data.paychan.forEach(function(d) {
    var key = utils.formatTime(d.time) +
      '|' + utils.padNumber(d.ledger_index, LI_PAD) +
      '|' + utils.padNumber(d.tx_index, I_PAD)

    var paychan = {
      'f:channel': d.channel,
      'f:tx_type': d.tx_type,
      'f:account': d.account,
      'f:owner': d.owner,
      'f:source': d.source,
      'f:destination': d.destination,
      'f:destination_tag': d.destination_tag,
      'f:source_tag': d.source_tag,
      'f:cancel_after': d.cancel_after,
      'f:expiration': d.expiration,
      amount: d.amount,
      balance: d.balance,
      settle_delay: d.settle,
      signature: d.signature,
      pubkey: d.pubkey,
      flags: d.flags,
      fee: d.fee,
      'f:tx_hash': d.tx_hash,
      'f:executed_time': d.time,
      'f:ledger_index': d.ledger_index,
      tx_index: d.tx_index,
      'f:client': d.client
    }

    tables.payment_channels[key] = paychan
    tables.account_payment_channels[d.source + '|' + key] = paychan
    tables.account_payment_channels[d.destination + '|' + key] = paychan
  })

  // add accounts created
  data.accountsCreated.forEach(function(a) {
    var key = utils.formatTime(a.time) +
      '|' + utils.padNumber(a.ledger_index, LI_PAD) +
      '|' + utils.padNumber(a.tx_index, I_PAD)

    tables.accounts_created[key] = {
      'f:account': a.account,
      'f:parent': a.parent,
      balance: a.balance,
      'f:tx_hash': a.tx_hash,
      'f:executed_time': a.time,
      'f:ledger_index': a.ledger_index,
      tx_index: a.tx_index,
      'f:client': a.client
    }
  })

  // add memos
  data.memos.forEach(function(m) {
    var key = utils.formatTime(m.executed_time) +
      '|' + utils.padNumber(m.ledger_index, LI_PAD) +
      '|' + utils.padNumber(m.tx_index, I_PAD) +
      '|' + utils.padNumber(m.memo_index, I_PAD)

    tables.memos[key] = {
      'f:account': m.account,
      'f:destination': m.destination,
      'f:source_tag': m.source_tag,
      'f:destination_tag': m.destination_tag,
      memo_type: m.memo_type,
      memo_data: m.memo_data,
      memo_format: m.memo_format,
      decoded_type: m.decoded_type,
      decoded_data: m.decoded_data,
      decoded_format: m.decoded_format,
      type_encoding: m.type_encoding,
      data_encoding: m.data_encoding,
      format_encoding: m.format_encoding,
      'f:tx_hash': m.tx_hash,
      'f:executed_time': m.executed_time,
      'f:ledger_index': m.ledger_index,
      tx_index: m.tx_index,
      memo_index: m.memo_index
    }

    tables.lu_account_memos[m.account + '|' + key] = {
      rowkey: key,
      'f:is_sender': true,
      'f:tag': m.source_tag,
      'f:tx_hash': m.tx_hash,
      'f:executed_time': m.executed_time,
      'f:ledger_index': m.ledger_index,
      tx_index: m.tx_index,
      memo_index: m.memo_index
    }

    if (m.destination) {
      tables.lu_account_memos[m.destination + '|' + key] = {
        rowkey: key,
        'f:is_source': false,
        'f:tag': m.destination_tag,
        'f:tx_hash': m.tx_hash,
        'f:executed_time': m.executed_time,
        'f:ledger_index': m.ledger_index,
        tx_index: m.tx_index,
        memo_index: m.memo_index
      }
    }
  })

  // add affected accounts
  data.affectedAccounts.forEach(function(a) {
    var key = a.account +
      '|' + utils.formatTime(a.time) +
      '|' + utils.padNumber(a.ledger_index, LI_PAD) +
      '|' + utils.padNumber(a.tx_index, I_PAD)

    tables.lu_affected_account_transactions[key] = {
      'f:type': a.tx_type,
      'f:result': a.tx_result,
      tx_hash: a.tx_hash,
      tx_index: a.tx_index,
      'f:executed_time': a.time,
      'f:ledger_index': a.ledger_index,
      'f:client': a.client
    }
  })

  return tables
}

/**
 * SaveParsedData
 */

HbaseClient.saveParsedData = function(params, callback) {
  var self = this
  var tables = self.prepareParsedData(params.data)
  var tableNames

  tableNames = params.tableNames ? params.tableNames : Object.keys(tables)

  Promise.map(tableNames, function(name) {
    return self.putRows({
      table: name,
      rows: tables[name]
    })
  })
  .nodeify(function(err, resp) {
    var total = 0
    if (err) {
      self.log.error('error saving parsed data', err)

    } else {
      if (resp) {
        resp.forEach(function(r) {
          if (r && r[0]) {
            total += r[0]
          }
        })
      }

      self.log.info('parsed data saved:', total + ' rows')
    }

    if (callback) {
      callback(err, total)
    }
  })
}

/**
 * removeLedger
 */

HbaseClient.removeLedger = function(hash, callback) {
  var self = this

  self.getLedger({
    ledger_hash: hash,
    transactions: true,
    expand: true,
    invalid: true

  }, function(err, ledger) {
    var parsed
    var primary
    var secondary
    var transactions
    var tables
    var table

    if (err) {
      self.log.error('error fetching ledger:', hash, err)
      callback(err)
      return
    }

    if (!ledger) {
      callback('ledger not found')
      return
    }

    // parser expects ripple epoch
    ledger.close_time -= EPOCH_OFFSET
    transactions = ledger.transactions
    ledger.transactions = []

    // ledgers must be formatted according to the output from
    // rippled's ledger command
    transactions.forEach(function(tx) {
      if (tx) {
        var transaction = tx.tx
        transaction.metaData = tx.meta
        transaction.hash = tx.hash
        ledger.transactions.push(transaction)
      }
    })

    parsed = Parser.parseLedger(ledger)
    primary = self.prepareLedgerTables(ledger)
    secondary = self.prepareParsedData(parsed)
    transactions = self.prepareTransactions(parsed.transactions)
    tables = []

    for (table in primary) {
      tables.push({
        table: table,
        keys: Object.keys(primary[table])
      })
    }

    for (table in transactions) {
      tables.push({
        table: table,
        keys: Object.keys(transactions[table])
      })
    }

    for (table in secondary) {
      tables.push({
        table: table,
        keys: Object.keys(secondary[table])
      })
    }

    Promise.map(tables, function(t) {
      return self.deleteRows({
        table: t.table,
        rowkeys: t.keys
      })
    }).nodeify(function(e, resp) {
      if (!e) {
        self.log.info('ledger removed:', ledger.ledger_index, hash)
      }

      callback(err, resp)
    })
  })
}

module.exports = HbaseClient
