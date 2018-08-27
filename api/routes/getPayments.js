'use strict'

var Logger = require('../../lib/logger')
var log = new Logger({scope: 'payments'})
var smoment = require('../../lib/smoment')
var utils = require('../../lib/utils')
var validator = require('ripple-address-codec')
var hbase = require('../../lib/hbase')

function getPayments(req, res) {

  var options = {
    start: smoment(req.query.start || 0),
    end: smoment(req.query.end),
    descending: (/true/i).test(req.query.descending) ? true : false,
    limit: Number(req.query.limit || 200),
    marker: req.query.marker,
    format: (req.query.format || 'json').toLowerCase()
  }

  var currency = req.params.currency


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
        message: 'unable to retrieve payments'
      })
    }
  }

 /**
  * successResponse
  * return a successful response
  * @param {Object} resp
  */

  function successResponse(resp) {
    var filename

    if (resp.marker) {
      utils.addLinkHeader(req, res, resp.marker)
    }

    if (options.format === 'csv') {
      filename = 'payments' +
        (resp.currency ? ' - ' + resp.currency : ' ') +
        (resp.issuer ? ' ' + resp.currency : ' ') + '.csv'
      resp.rows.forEach(function(r, i) {
        resp.rows[i] = utils.flattenJSON(r)
      })
      res.csv(resp.rows, filename)

    // json
    } else {
      res.json({
        result: 'success',
        currency: resp.currency,
        issuer: resp.issuer,
        count: resp.rows.length,
        marker: resp.marker,
        payments: resp.rows
      })
    }
  }

  if (currency) {
    currency = currency.split(/[\+|\.]/)  // any of +, |, or .
    options.currency = currency[0].toUpperCase()
    options.issuer = currency[1]
  }

  if (options.issuer && !validator.isValidAddress(options.issuer)) {
    errorResponse({error: 'invalid issuer address', code: 400})
    return

  } else if (!options.start) {
    errorResponse({error: 'invalid start date format', code: 400})
    return

  } else if (!options.end) {
    errorResponse({error: 'invalid end date format', code: 400})
    return

  } else if (options.currency &&
             options.currency !== 'XRP' &&
            !options.issuer) {
    errorResponse({error: 'issuer is required', code: 400})
    return
  }

  if (isNaN(options.limit)) {
    options.limit = 200

  } else if (options.limit > 200) {
    options.limit = 200
  }

  hbase.getPayments(options, function(err, resp) {
    if (err || !resp) {
      errorResponse(err)
      return
    }

    resp.rows.forEach(function(r) {
      delete r.rowkey
      r.executed_time = smoment(r.executed_time).format()
      r.transaction_cost = r.fee
      delete r.fee
      delete r.rowkey
      delete r.client
    })

    successResponse(resp)
  })
}

module.exports = getPayments
