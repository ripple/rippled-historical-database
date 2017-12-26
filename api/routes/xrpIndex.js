'use strict';

const config = require('../../config')
const Logger = require('../../lib/logger')
const log = new Logger({scope : 'xrp index'})
const hbase = require('../../lib/hbase')
const smoment = require('../../lib/smoment')
const moment = require('moment')
const request = require('request-promise')
const utils = require('../../lib/utils')

const intervals = [
  '5minute',
  '15minute',
  '30minute',
  '1hour',
  '2hour',
  '4hour',
  '1day'
]

module.exports = function(req, res) {
  return validate(req.query)
  .then(options => {
    return getRate(options.currency)
    .then(rate => {
      if (!rate) {
        return Promise.reject({
          error: 'exchange rate unavailable',
          code: 400
        })
      }

      return getIndex(options, rate)
    })
  })
  .then(result => {

    if (result.marker) {
      utils.addLinkHeader(req, res, result.marker);
    }

    res.send({
      result: 'success',
      count: result.rows.length,
      fx_rate: result.rate !== 1 ?
        result.rate.toString() : undefined,
      rows: result.rows,
      marker: result.marker
    })
  })
  .catch(err => {
    log.error(err.error || err)
    res.status(err.code || 500).json({
      result: 'error',
      message: err.error || err
    })
  })
}

/**
 * getInverseTimestamp
 */

function getInverseTimestamp(date) {
  return 99999999999999 - Number(date.format('YYYYMMDDHHmmss'))
}

/**
 * getRate
 */

function getRate(currency) {

  if (currency === 'USD') {
    return Promise.resolve(1)
  }

  return new Promise((resolve, reject) => {
    hbase.getScan({
      table: 'forex_rates',
      startRow: 'USD|' + currency,
      stopRow: 'USD|' + currency + '|z',
      descending: true,
      limit: 1
    }, (err, res) => {
      if (err) {
        reject(err)


      } else if (res[0] && moment().diff(res[0].date, 'minutes') < 120) {
        resolve(Number(res[0].rate))

      } else {
        resolve()
      }
    })
  })
}


/**
 * validate
 */

function validate(params) {

  const options = {
    start: smoment(params.start || '2013-01-01'),
    end: smoment(params.end),
    interval: (params.interval || '').toLowerCase(),
    currency: (params.currency || 'USD').toUpperCase(),
    limit: params.limit,
    marker: params.marker,
    ascending: (/true/i).test(params.descending) ? false : true,
  }

  if (!options.start) {
    return Promise.reject({
      error: 'invalid start date format',
      code: 400
    })
  }

  if (!options.end) {
    return Promise.reject({
      error: 'invalid end date format',
      code: 400
    })
  }

  if (options.interval && intervals.indexOf(options.interval) === -1) {
    return Promise.reject({
      error: 'invalid interval',
      code: 400
    })
  }

  if (isNaN(options.limit)) {
    options.limit = 200;

  } else if (options.limit > 1000) {
    options.limit = 1000;
  }

  return Promise.resolve(options)
}

/**
 * getBook
 */

function getIndex(options, rate) {
  return new Promise((resolve, reject) => {
    const table = options.interval ?
      'agg_xrp_index' : 'xrp_index'

    const base = options.interval ?
      options.interval + '|' : ''
    const stop = getInverseTimestamp(options.start)
    const start = getInverseTimestamp(options.end)

    hbase.getScanWithMarker(hbase, {
      table: table,
      startRow: base + start,
      stopRow: base + stop,
      limit: options.limit,
      marker: options.marker,
      descending: !options.ascending,
      columns: [
        'd:midpoint',
        'd:volume',
        'd:usd_volume',
        'd:count',
        'd:open',
        'd:high',
        'd:low',
        'd:close',
        'd:vwap',
        'd:date',
        'f:date'
      ]
    },
    function(err, resp) {
      if (err) {
        reject(err)
        return
      }

      const rows = []
      resp.rows.forEach(row => {
        if (row.midpoint) {
          rows.push({
            price: (row.midpoint * rate).toPrecision(6),
            volume: row.volume,
            counter_volume: (row.volume * row.midpoint * rate).toString(),
            count: Number(row.count || 0),
            date: row.date
          })

        } else {
          rows.push({
            open: row.open ? (row.open * rate).toPrecision(6) : 0,
            high: row.high ? (row.high * rate).toPrecision(6) : 0,
            low: row.low ? (row.low * rate).toPrecision(6) : 0,
            close: row.close ? (row.close * rate).toPrecision(6) : 0,
            vwap: row.vwap ? (row.vwap * rate).toPrecision(6) : 0,
            volume: row.volume,
            counter_volume: (row.usd_volume * rate).toString(),
            count: Number(row.count || 0),
            date: row.date
          })
        }
      })

      resolve({
        marker: resp.marker,
        rows: rows,
        rate: rate
      })
    })
  })
}

