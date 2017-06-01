'use strict'

var async = require('async')
var Promise = require('bluebird')
var smoment = require('../lib/smoment')
var config = require('../config')
var hbase = require('../lib/hbase')

var intervals = [
  'hour',
  'day',
  'week',
  'month'
]

var periods = [
  'minute',
  'hour',
  'day'
]

var options = {
  interval: config.get('interval'),
  start: config.get('start'),
  end: config.get('end'),
  save: config.get('save'),
  live: config.get('live'),
  summary: config.get('summary')
}

if (!options.save && !options.summary) {
  config.set('logLevel', 1)
}

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
        date = smoment(params.start)
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

        } else {
          currencyList.push({
            currency: 'XRP'
          })

          resolve(currencyList)
        }
      })
    })
  }

  /**
   * getVolumes
   * get payment volume for each currency
   */

  function getVolumes(currencyList) {
    return new Promise(function(resolve, reject) {
      async.map(currencyList, function(c, asyncCallbackPair) {

        var currency = {
          currency: c.currency,
          issuer: c.issuer
        }

        hbase.getPayments({
          currency: c.currency,
          issuer: c.issuer,
          start: smoment(params.start),
          end: smoment(params.end || params.start),
          interval: params.interval,
          reduce: params.interval ? false : true,
          descending: false
        },
        function(err, data) {

          if (err) {
            asyncCallbackPair(err)
            return
          }

          if (params.interval && data) {
            currency.amount = data.rows && data.rows[0] ?
              data.rows[0].amount : 0
            currency.count = data.rows && data.rows[0] ?
              data.rows[0].count : 0
          } else if (data) {
            currency.amount = data.amount
            currency.count = data.count
          } else {
            currency.amount = 0
            currency.count = 0
          }

          asyncCallbackPair(null, currency)
        })
      }, function(err, resp) {
        if (err) {
          reject(err)

        // filter rows with no payments
        } else {
          resolve(resp.filter(function(d) {
            return Boolean(d.count)
          }))
        }
      })
    })
  }


  /**
   * getRates
   */

  function getRates(data) {

    return new Promise(function(resolve, reject) {

      // get exchanges for each pair
      async.map(data, function(d, asyncCallbackPair) {

        if (d.currency === 'XRP') {
          d.rate = 1
          asyncCallbackPair(null, d)
          return
        }

        hbase.getExchangeRate({
          base: {
            currency: 'XRP'
          },
          counter: {
            currency: d.currency,
            issuer: d.issuer
          },
          date: params.start
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

  /**
   * normalize
   */

  function normalize(data) {
    var total = 0
    var count = 0

    data.forEach(function(d) {
      d.converted_amount = d.rate ? d.amount / d.rate : 0
      total += d.converted_amount
      count += d.count
    })

    return {
      startTime: smoment(params.start).format(),
      total: total,
      count: count,
      exchange: {currency: 'XRP'},
      exchangeRate: 1,
      components: data.filter(function(d) {
        return Boolean(d.converted_amount)
      }).sort(function(a, b) {
        return b.converted_amount - a.converted_amount
      })
    }
  }

  /**
   * save
   */

  function save(result) {
    return new Promise(function(resolve, reject) {
      if (options.save) {

        var table = 'agg_metrics'
        var rowkey = 'payment_volume|'

        if (params.live === 'day') {
          rowkey += 'live'
        } else if (params.live) {
          rowkey += 'live|' + params.live
        } else {
          rowkey += options.interval + '|' +
            smoment(result.startTime).hbaseFormatStartRow()
        }

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
            date: result.startTime,
            total: result.total,
            count: result.count
          })
        }

        resolve(result)
      }
    })
  }

  getCurrencies()
    .then(getVolumes)
    .then(getRates)
    .then(normalize)
    .then(save)
    .nodeify(done)
}

/**
 * aggregatePayments
 */

function aggregatePayments(params) {

  var list = []
  var start
  var end
  var interval

  if (params.live === true) {
    params.live = 'day'
  }

  interval = (params.interval || '').toLowerCase()
  start = smoment(params.start)
  end = smoment(params.end)

  // invalid start date
  // or end date
  if (!start || !end) {
    console.log('invalid start or end time')
    process.exit(1)
  }

  // invalid interval
  if (interval && intervals.indexOf(interval) === -1) {
    console.log('invalid interval:', interval)
    process.exit(1)

  // history, multiple
  } else if (interval && params.end) {
    start.moment.startOf(interval === 'week' ? 'isoWeek' : interval)
    while (end.moment.diff(start.moment) >= 0) {
      list.push({
        interval: interval,
        start: smoment(start)
      })

      start.moment.add(1, interval)
    }

  // history, single
  } else if (interval) {
    start.moment.startOf(interval === 'week' ? 'isoWeek' : interval)
    list.push({
      interval: interval,
      start: start
    })

  // invalid live period
  } else if (params.live && periods.indexOf(params.live) === -1) {
    console.log('invalid live period:', params.live)
    process.exit(1)

  // live hourly
  } else if (params.live) {
    start = smoment()
    start.moment.subtract(1, params.live)
    list.push({
      start: start,
      end: smoment(),
      live: params.live
    })

  // live (24hrs)
  } else {
    start = smoment()
    start.moment.subtract(24, 'hours')
    list.push({
      start: start,
      end: smoment(),
      live: 'day'
    })
  }

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

aggregatePayments(options)
