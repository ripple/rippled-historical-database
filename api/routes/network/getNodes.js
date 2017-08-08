'use strict'

var Logger = require('../../../lib/logger')
var log = new Logger({scope: 'topology nodes'})
var smoment = require('../../../lib/smoment')
var hbase = require('../../../lib/hbase')

function getNodes(req, res) {
  var options = {
    pubkey: req.params.pubkey,
    date: smoment(req.query.date),
    details: (/true/i).test(req.query.verbose) ? true : false,
    limit: Number(req.query.limit || 200),
    format: (req.query.format || 'json').toLowerCase()
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
        message: 'unable to retrieve topology node(s)'
      })
    }
  }

  /**
  * successResponse
  * return a successful response
  * @param {Object} markets
  * @param {Object} options
  */

  function successResponse(data) {

    if (options.pubkey) {
      data.result = 'success'
      res.json(data)

    } else {
      var filename

      if (options.format === 'csv') {
        filename = 'topology nodes - ' + data.date + '.csv'
        res.csv(data.nodes, filename)

      } else {
        res.json({
          result: 'success',
          date: data.date,
          count: data.nodes.length,
          nodes: data.nodes
        })
      }
    }
  }

  if (req.query.date && !options.date) {
    errorResponse({
      error: 'invalid date format',
      code: 400
    })
    return
  }

  if (isNaN(options.limit)) {
    options.limit = 200

  } else if (options.limit > 1000) {
    options.limit = 1000
  }

  log.info(options.pubkey || options.date.format())

  hbase.getTopologyNodes(options)
  .nodeify(function(err, resp) {
    if (err) {
      errorResponse(err)

    } else if (!resp) {
      errorResponse({
        error: 'node not found',
        code: 404
      })

    } else {
      successResponse(resp)
    }
  })
}

module.exports = getNodes
