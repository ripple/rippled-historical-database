'use strict'

var Logger = require('../../lib/logger')
var log = new Logger({scope: 'health check'})
var moment = require('moment')
var hbase = require('../../lib/hbase')

var defaults = {
  api: {
    threshold1: 5
  },
  importer: {
    threshold1: 60 * 5,
    threshold2: 60 * 15
  },
  validations_etl: {
    threshold1: 60 * 2
  },
  nodes_etl: {
    threshold1: 60 * 2
  }
}

var aspects = Object.keys(defaults)

// function for formatting duration
function duration(ms) {
  if (ms === Infinity) {
    return ms.toString()
  }

  var s = Math.floor(ms / 1000)
  var years = Math.floor(s / 31536000)
  if (years) {
    return (s / 31536000).toFixed(2) + 'y'
  }

  var days = Math.floor((s %= 31536000) / 86400)
  if (days) {
    return ((s %= 31536000) / 86400).toFixed(2) + 'd'
  }

  var hours = Math.floor((s %= 86400) / 3600)
  if (hours) {
    return ((s %= 86400) / 3600).toFixed(2) + 'h'
  }

  var minutes = Math.floor((s %= 3600) / 60)
  if (minutes) {
    return ((s %= 3600) / 60).toFixed(2) + 'm'
  }

  return ms / 1000 + 's'
}

/**
 * checkHealth
 */

function checkHealth(req, res) {

  var aspect = (req.params.aspect || 'api').toLowerCase()
  var verbose = (/true/i).test(req.query.verbose) ? true : false
  var t1
  var t2

  var d = Date.now()

  /**
   * nodeHealthCheck
   */

  function nodeHealthCheck() {
    hbase.getTopologyInfo()
    .then(function(info) {

      var parts = info ? info.rowkey.split('_') : undefined
      var gap = info ? (Date.now() - parts[0]) / 1000 : Infinity
      var score = gap <= t1 ? 0 : 1

      if (verbose) {
        res.json({
          score: score,
          gap: duration(gap * 1000),
          gap_threshold: duration(t1 * 1000),
          message: score ? 'last imported data exceeds threshold' : undefined
        })
      } else {
        res.send(score.toString())
      }
    }).catch(function(err) {
      log.error(err)
      res.status(500).json({
        result: 'error',
        message: 'hbase response error'
      })
    })
  }

  /**
   * validationHealthCheck
   */

  function validationHealthCheck() {
    hbase.getScan({
      table: 'validations_by_date',
      startRow: 0,
      stopRow: '~',
      descending: true,
      limit: 1
    }, function(err, resp) {

      if (err) {
        log.error(err)
        res.status(500).json({
          result: 'error',
          message: 'hbase response error'
        })
        return
      }

      var last = resp && resp.length ?
        moment(resp[0].datetime) : null
      var gap = last ?
        (Date.now() - last.unix() * 1000) / 1000 : Infinity
      var score = gap <= t1 ? 0 : 1

      if (verbose) {
        res.json({
          score: score,
          gap: duration(gap * 1000),
          gap_threshold: duration(t1 * 1000),
          message: score ? 'last imported data exceeds threshold' : undefined
        })

      } else {
        res.send(score.toString())
      }
    })
  }

  /**
   * apiHealthResponse
   */

  function apiHealthResponse(responseTime, err) {
    var score
    var message

    if (err) {
      log.error(err)
      res.status(500).json({
        result: 'error',
        message: 'hbase response error'
      })
      return

    } else if (responseTime < 0 || isNaN(responseTime)) {
      score = 2
      message = 'invalid response time'
    } else if (responseTime > t1) {
      score = 1
      message = 'response time exceeds threshold'
    } else {
      score = 0
    }

    if (verbose) {
      res.json({
        score: score,
        response_time: duration(responseTime * 1000),
        response_time_threshold: duration(t1 * 1000),
        message: message
      })

    } else {
      res.send(score.toString())
    }
  }

  /**
   * importerHealthResponse
   */

  function importerHealthResponse(responseTime, ledgerGap, e) {

    // get last validated ledger
    hbase.getLastValidated(function(err, resp) {
      var now = Date.now()
      var closeTime = resp && resp.close_time ?
          moment.utc(resp.close_time) : undefined
      var validatorGap = closeTime ?
          (now - (closeTime.unix() * 1000)) / 1000 : Infinity
      var score
      var message

      if (e || err) {
        log.error(e || err)
        res.status(500).json({
          result: 'error',
          message: 'hbase response error'
        })
        return

      } else if (responseTime < 0 || isNaN(responseTime)) {
        score = 3
        message = 'invalid response time'
      } else if (ledgerGap > t1) {
        score = 2
        message = 'last ledger gap exceeds threshold'
      } else if (validatorGap > t2) {
        score = 1
        message = 'last validation gap exceeds threshold'
      } else {
        score = 0
      }

      if (verbose) {
        res.json({
          score: score,
          response_time: duration(responseTime * 1000),
          ledger_gap: duration(ledgerGap * 1000),
          ledger_gap_threshold: duration(t1 * 1000),
          validation_gap: duration(validatorGap * 1000),
          validation_gap_threshold: duration(t2 * 1000),
          last_validated_ledger: resp ? Number(resp.ledger_index) : undefined,
          message: message
        })

      } else {
        res.send(score.toString())
      }
    })
  }

  if (aspects.indexOf(aspect) === -1) {
    res.status(400).json({
      result: 'error',
      message: 'invalid aspect type'
    })
    return
  }

  t1 = Number(req.query.threshold || defaults[aspect].threshold1 || 0)
  t2 = Number(req.query.threshold2 || defaults[aspect].threshold2 || 0)

  if (isNaN(t1) || isNaN(t2)) {
    res.status(400).json({
      result: 'error',
      message: 'invalid threshold'
    })
    return
  }

  log.info(aspect)

  if (aspect === 'nodes_etl') {
    nodeHealthCheck()

  } else if (aspect === 'validations_etl') {
    validationHealthCheck()

  } else {
    hbase.getLedger({}, function(err, ledger) {
      var now = Date.now()
      var gap = ledger ? (now - ledger.close_time * 1000) / 1000 : 0
      var responseTime = (Date.now() - d) / 1000

      if (aspect === 'api') {
        apiHealthResponse(responseTime, err)
      } else {
        importerHealthResponse(responseTime, gap, err)
      }
    })
  }
}

module.exports = checkHealth
