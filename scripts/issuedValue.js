'use strict'

var async = require('async')
var Promise = require('bluebird')
var smoment = require('../lib/smoment')
var config = require('../config/import.config')
var Hbase = require('../lib/hbase/hbase-client')
var hbaseOptions = config.get('hbase')
var hbase

var options = {
  start: config.get('start'),
  end: config.get('end'),
  save: config.get('save'),
  summary: config.get('summary')
}

if (!options.save && !options.summary) {
  hbaseOptions.logLevel = 1
}

hbase = new Hbase(hbaseOptions)

/**
 * handleAggregation
 */

function handleAggregation(params, done) {

  function getCurrencies() {
    return new Promise(function(resolve, reject) {
      var date
      var max

      if (!params.live) {
        max = smoment()
        max.moment.subtract(2, 'days').startOf('day')

        // get the date at the end
        // of the provided interval
        date = smoment(params.time)
        date.moment.startOf(params.interval)

        // use max if the date
        // provided is later than that
        if (date.moment.diff(max.moment) > 0) {
          date = max
        }
      }

      hbase.getTopCurrencies({date: date}, function(err, currencyList) {

        if (err) {
          reject(err)

        // no markets found
        } else if (!currencyList.length) {
          reject('no markets found')

        } else {
          resolve(currencyList)
        }
      })
    })
  }

  function getCapitalization(currencyList) {

    return new Promise(function(resolve, reject) {

      // get capitalization data for each currency
      async.map(currencyList, function(c, asyncCallbackPair) {

        hbase.getCapitalization({
          currency: c.currency,
          issuer: c.issuer,
          start: smoment(0),
          end: smoment(params.time),
          descending: true,
          adjusted: true,
          limit: 1
        }, function(err, resp) {

          if (err) {
            asyncCallbackPair(err)
            return
          }

          asyncCallbackPair(null, {
            currency: c.currency,
            issuer: c.issuer,
            amount: resp.rows[0] ? resp.rows[0].amount : 0
          })
        })
      }, function(err, resp) {
        if (err) {
          reject(err)
        } else {
          resolve(resp)
        }
      })
    })
  }

  function getRates(data) {

    return new Promise(function(resolve, reject) {

      // get exchanges for each pair
      async.map(data, function(d, asyncCallbackPair) {
        hbase.getExchangeRate({
          base: {
            currency: 'XRP'
          },
          counter: {
            currency: d.currency,
            issuer: d.issuer
          },
          date: smoment(params.time)
        })
        .then(function(resp) {

          d.rate = resp || 0

          asyncCallbackPair(null, d)
        }).catch(function(err) {
          asyncCallbackPair(err)
        })

      }, function(err, resp) {
        if (err) {
          reject(err)
        } else {
          resolve(resp)
        }
      })
    })
  }

  function normalize(data) {
    var total = 0
    data.forEach(function(d) {
      d.converted_amount = d.rate ? d.amount / d.rate : 0
      total += d.converted_amount
    })

    return {
      time: smoment(params.time).format(),
      total: total,
      exchange: {currency: 'XRP'},
      exchangeRate: 1,
      components: data
    }
  }

  function save(result) {
    return new Promise(function(resolve, reject) {

      var table = 'agg_metrics'
      var rowkey = 'issued_value|'

      if (options.save) {
        rowkey += params.live ? 'live' : params.time.hbaseFormatStartRow()

        console.log('saving:', table, rowkey)
        hbase.putRow({
          table: table,
          rowkey: rowkey,
          columns: result
        })
        .nodeify(function(err) {
          if (err) {
            reject(err)
          } else {
            delete result.components
            console.log('saved:', table, rowkey)
            console.log(result)
            resolve(rowkey)
          }
        })

      } else {
        if (options.summary) {
          console.log({
            components: result.components.length,
            date: result.time,
            total: result.total
          })
        }

        resolve(result)
      }
    })
  }

  getCurrencies()
    .then(getCapitalization)
    .then(getRates)
    .then(normalize)
    .then(save)
    .nodeify(done)
}

/**
 * aggregateIssuedValue
 */

function aggregateIssuedValue(params) {

  var list = []
  var start
  var end

  if (params.end && !params.start) {
    console.log('start date is required')
    process.exit(1)

  } else if (params.start && params.end) {
    start = smoment(params.start)
    end = smoment(params.end)

    if (!start || !end) {
      console.log('invalid start or end date')
      process.exit(1)
    }

  } else if (params.start) {
    start = smoment(params.start)
    end = smoment(params.start)

    if (!start) {
      console.log('invalid start date')
      process.exit(1)
    }
  }

  if (start && end) {
    start.moment.startOf('day')
    start.granularity = 'day'
    end.granularity = 'day'

    while (end.moment.diff(start.moment) >= 0) {
      list.push({
        time: smoment(start.format())
      })

      start.moment.add(1, 'day')
    }

  } else {
    list.push({
      live: true
    })
  }

  // aggregate each in series
  async.mapSeries(list, handleAggregation, function(err, resp) {
    var data

    if (err) {
      console.log(err)

    // print to stdout
    } else if (!options.save && !options.summary) {
      data = resp.length === 1 ? resp[0] : resp
      process.stdout.write(JSON.stringify(data, null, 2) + '\n')
    }

    process.exit(err ? 1 : 0)
  })
}

// run aggregations
aggregateIssuedValue(options)
