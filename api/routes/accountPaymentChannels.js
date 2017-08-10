'use strict'

var Logger = require('../../lib/logger')
var log = new Logger({scope: 'account payment channels'})
var smoment = require('../../lib/smoment')
var utils = require('../../lib/utils')
var hbase = require('../../lib/hbase')

/**
 * AccountPaymentChannels
 */

function AccountPaymentChannels(req, res) {
  var params

  /**
   * prepareOptions
   */

  function prepareOptions() {
    var options = {
      account: req.params.address,
      start: smoment(req.query.start || '2017-01-01'),
      end: smoment(req.query.end),
      type: req.query.type,
      destination: req.query.destination,
      destination_tag: req.query.destination_tag,
      source_tag: req.query.source_tag,
      marker: req.query.marker,
      descending: (/true/i).test(req.query.descending) ? true : false,
      limit: Number(req.query.limit) || 200,
      format: (req.query.format || 'json').toLowerCase()
    }

    if (!options.start) {
      return {error: 'invalid start date format', code: 400}
    } else if (!options.end) {
      return {error: 'invalid end date format', code: 400}
    }

    if (!options.account) {
      return {error: 'Account is required', code: 400}
    }

    if (isNaN(options.limit)) {
      options.limit = 200

    } else if (options.limit > 1000) {
      options.limit = 1000
    }

    return options
  }

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
        message: 'unable to retrieve payment channel transactions'
      })
    }
  }

  /**
  * successResponse
  * return a successful response
  * @param {Object} payments
  */

  function successResponse(data) {
    var filename = params.account + ' - payment channels'
    var results = []

    if (data.marker) {
      utils.addLinkHeader(req, res, data.marker)
    }

    if (params.format === 'csv') {
      data.rows.forEach(function(r) {
        results.push(utils.flattenJSON(r))
      })

      res.csv(results, filename + '.csv')
    } else {
      res.json({
        result: 'success',
        count: data.rows.length,
        marker: data.marker,
        rows: data.rows
      })
    }
  }

  params = prepareOptions()

  if (params.error) {
    errorResponse(params)
    return

  } else {
    log.info('get: ' + params.account)

    hbase.getAccountPaymentChannels(params, function(err, data) {
      if (err) {
        errorResponse(err)
      } else {
        data.rows.forEach(function(tx) {
          tx.executed_time = smoment(tx.executed_time).format()
          tx.transaction_cost = tx.fee
          delete tx.fee
          delete tx.rowkey
          delete tx.client
        })

        successResponse(data)
      }
    })
  }
}

module.exports = AccountPaymentChannels
