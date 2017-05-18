'use strict'

var async = require('async')
var Promise = require('bluebird')
var smoment = require('../lib/smoment')
var config = require('../config/import.config')
var Hbase = require('../lib/hbase')
var hbaseOptions = config.get('hbase')
var hbase

var intervals = [
  'hour',
  'day',
  'week',
  'month'
]

var periods = [
  'minute',
  'hour',
  'day',
  '3day',
  '7day',
  '30day'
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
  hbaseOptions.logLevel = 1
}

// long timeout needed
// for month interval
hbaseOptions.timeout = 120000
hbase = new Hbase(hbaseOptions)

/**
 * handleAggregation
 */

function handleAggregation(params, done) {

  /**
   * getMarkets
   * get markets either from the list here or
   * hbase top markets
   */

  function getMarkets() {
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

      hbase.getTopMarkets({date: date}, function(err, markets) {

        if (err) {
          reject(err)

        // format results
        } else {
          markets.forEach(function(market) {
            market.base = {
              currency: market.base_currency,
              issuer: market.base_issuer
            }
            market.counter = {
              currency: market.counter_currency,
              issuer: market.counter_issuer
            }
          })
          resolve(markets)
        }
      })
    })
  }

  function reduce(rows) {
    var reduced = {
      base_volume: 0,
      counter_volume: 0,
      count: 0,
      vwap: 0
    }

    rows.forEach(function(d) {
      reduced.base_volume += d.base_volume
      reduced.counter_volume += d.counter_volume
      reduced.count += d.count
    })

    if (reduced.base_volume) {
      reduced.vwap = reduced.counter_volume / reduced.base_volume
    }
    return reduced
  }

  /**
   * getVolumes
   * get trade volume for each market
   */

  function getVolumes(markets) {


    markets.forEach(function(market) {
      var swap

      if (market.base.currency === 'XRP') {
        swap = market.base
        market.base = market.counter
        market.counter = swap
      }
    })

    return Promise.map(markets, function(market) {
      return new Promise(function(resolve, reject) {
        var start = smoment(params.start)
        var end
        var interval

        if (params.live &&
            ['3day', '7day', '30day'].indexOf(params.live) !== -1) {
          interval = '5minute'
          end = smoment(params.end)

        } else {
          end = smoment(params.start)
          end.moment.add(1, params.interval || 'day').subtract(1, 'second')
        }

        hbase.getExchanges({
          base: market.base,
          counter: market.counter,
          start: start,
          end: end,
          interval: interval,
          limit: Infinity,
          reduce: true
        }, function(err, resp) {

          var data = {}

          if (err) {
            reject(err)
            return
          }

          if (interval) {
            resp.reduced = reduce(resp.rows)
          }

          data.count = resp.reduced.count
          data.rate = resp.reduced.vwap
          data.amount = resp.reduced.base_volume
          data.base = market.base
          data.counter = market.counter

          if (data.counter.currency === 'XRP') {
            data.converted_amount = resp.reduced.counter_volume
            data.rate = resp.reduced.vwap ?
              1 / resp.reduced.vwap : 0
          }

          resolve(data)
        })
      })
    })
  }

  /**
   * getRates
   * get XRP conversion rates from the results
   */

  function getRates(markets) {
    var data = {
      markets: markets,
      rates: {}
    }

    data.markets.forEach(function(market) {
      var key = market.base.currency + '.' + market.base.issuer
      if (market.counter.currency === 'XRP') {
        data.rates[key] = market.rate
      }
    })

    return data
  }

  /**
   * normalize
   * convert the amounts to XRP using the rates
   * found.  If we don't have a rate, it wont be included
   */

  function normalize(data) {
    var total = 0
    var count = 0

    data.markets.forEach(function(market) {
      var base = market.base.currency + '.' + market.base.issuer
      var counter = market.counter.currency + '.' + market.counter.issuer
      var swap

      // no trades or already determined
      if (!market.count || market.converted_amount) {
        return

      } else if (data.rates[base]) {
        market.converted_amount = market.amount / data.rates[base]
        market.rate /= data.rates[base]

      } else if (data.rates[counter]) {
        swap = market.base
        market.base = market.counter
        market.counter = swap
        market.amount = market.rate * market.amount
        market.converted_amount = market.amount / data.rates[counter]
        market.rate /= data.rates[counter]

      } else {
        console.log('no rate for:', base, counter)
      }
    })

    data.markets = data.markets.filter(function(market) {
      return market.count && market.converted_amount
    })

    data.markets.sort(function(a, b) {
      return b.converted_amount - a.converted_amount
    })

    data.markets.forEach(function(market) {
      total += market.converted_amount
      count += market.count
    })

    return {
      startTime: params.start.moment.format(),
      exchange: {currency: 'XRP'},
      exchangeRate: 1,
      total: total,
      count: count,
      components: data.markets
    }
  }

  /**
   * save
   * save the row to hbase
   */

  function save(result) {
    return new Promise(function(resolve, reject) {

      var table = 'agg_metrics'
      var rowkey = 'trade_volume|'

      if (options.save) {

        if (params.live === 'day') {
          rowkey += 'live'
        } else if (params.live) {
          rowkey += 'live|' + params.live
        } else {
          rowkey += params.interval + '|' + params.start.hbaseFormatStartRow()
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

  getMarkets()
    .then(getVolumes)
    .then(getRates)
    .then(normalize)
    .then(save)
    .nodeify(done)
}


/**
 * aggregateTradeVolume
 */

function aggregateTradeVolume(params) {

  var list = []
  var start
  var end
  var interval

  interval = (params.interval || '').toLowerCase()
  start = smoment(params.start)
  end = smoment(params.end)
  start.granularity = 'second'

  if (params.live === true) {
    params.live = 'day'
  }

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
        start: smoment(start.format())
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

  } else if (params.live === '3day') {
    start = smoment()
    start.moment.subtract(3, 'day')
    list.push({
      start: start,
      end: smoment(),
      live: params.live
    })

  } else if (params.live === '7day') {
    start = smoment()
    start.moment.subtract(7, 'day')
    list.push({
      start: start,
      end: smoment(),
      live: params.live
    })

  } else if (params.live === '30day') {
    start = smoment()
    start.moment.subtract(30, 'day')
    list.push({
      start: start,
      end: smoment(),
      live: params.live
    })

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
aggregateTradeVolume(options)
