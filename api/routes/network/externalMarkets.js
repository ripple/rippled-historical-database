'use strict'

var Logger = require('../../../lib/logger')
var log = new Logger({scope: 'external markets'})

var periods = [
  'hour',
  'day',
  '3day',
  '7day',
  '30day'
]

var hbase

/**
 * getExternalMarketData
 */

function getExternalMarketData(req, res) {
  var period = req.query.period || 'day'
  var base = 'trade_volume|external|live|'

  /**
  * errorResponse
  * return an error response
  * @param {Object} err
  */

  function errorResponse(err) {
    log.error(err.error || err)
    if (err.code && err.code.toString()[0] === '4') {
      res.status(err.code).json({
        result: 'error',
        message: err.error
      })
    } else {
      res.status(500).json({
        result: 'error',
        message: 'unable to retrieve market data'
      })
    }
  }

  if (period === 'day' || period === 'hour') {
    period = '1' + period

  } else if (periods.indexOf(period) === -1) {
    errorResponse({
      code: 400,
      error: 'invalid period - use: ' + periods.join(', ')
    })
    return
  }

  /**
  * successResponse
  * return a successful response
  * @param {Object} markets
  */

  function successResponse(data) {
    res.json({
      result: 'success',
      data: data
    })
  }

  hbase.getRow({
    table: 'agg_metrics',
    rowkey: base + period
  }, function(err, resp) {

    if (err) {
      errorResponse(err)

    } else if (!resp) {
      errorResponse('row not found')

    } else {
      delete resp.rowkey
      resp.components = JSON.parse(resp.components)
      resp.components.forEach(function(c) {
        c.base_volume = c.base_volume.toString()
        if (c.counter_volume) {
          c.counter_volume = c.counter_volume.toString()
        }

        if (c.vwap) {
          c.rate = c.vwap.toString()
          delete c.vwap
        }
      })
      successResponse(resp)
    }
  })
}

module.exports = function(db) {
  hbase = db
  return getExternalMarketData
}
