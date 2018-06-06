'use strict';

const config = require('../../config')
const Logger = require('../../lib/logger')
const log = new Logger({scope : 'xrp index'})
const hbase = require('../../lib/hbase')
const smoment = require('../../lib/smoment')
const moment = require('moment')
const request = require('request-promise')

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

      return getTicker(options, rate)
    })
  })
  .then(result => {
    result.result = 'success'
    res.send(result)
  })
  .catch(err => {
    log.error(err.error || err)
    res.status(isNaN(err.code) ? 500 : err.code).json({
      result: 'error',
      message: 'unable to get XRP index tickerdata'
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
    currency: (params.currency || 'USD').toUpperCase()
  }

  return Promise.resolve(options)
}

/**
 * getBook
 */

function getTicker(options, rate) {
  return new Promise((resolve, reject) => {
    hbase.getScan({
      table: 'agg_xrp_index',
      startRow: 'live|',
      stopRow: 'live|z',
    },
    function(err, rows) {
      const result = {}
      if (err) {
        reject(err)
        return
      }

      rows.forEach(row => {
        const key = row.rowkey.split('|')[1]
        delete row.rowkey
        result[key] = {
          open: (row.open * rate).toPrecision(6),
          high: (row.high * rate).toPrecision(6),
          low: (row.low * rate).toPrecision(6),
          close: (row.close * rate).toPrecision(6),
          vwap: (row.vwap * rate).toPrecision(6),
          volume: row.volume,
          counter_volume: (row.usd_volume * rate).toString(),
          count: Number(row.count || 0),
          date: row.date
        }
      })

      if (rate !== 1) {
        result.fx_rate = rate.toString()
      }

      resolve(result)
    })
  })
}

