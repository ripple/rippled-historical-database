'use strict'

var request = require('request-promise')
var smoment = require('../lib/smoment')
var moment = require('moment')
var config = require('../config/import.config')
var Hbase = require('../lib/hbase/hbase-client')
var hbase = new Hbase(config.get('hbase'))
var table = 'agg_exchanges_external'

/**
 * round
 * round to siginficant digits
 */

function round(n, sig) {
  var mult = Math.pow(10,
      sig - Math.floor(Math.log(n) / Math.LN10) - 1)
  return Math.round(n * mult) / mult
}

/**
 * getBitstamp
 */

function getBitstamp(currency) {

  var pair = ('xrp' + currency).toLowerCase()
  var url = 'https://www.bitstamp.net/api/v2/transactions/' + pair


  return request({
    url: url,
    json: true,
    qs: {
      time: 'hour'
    }
  }).then(function(resp) {
    var buckets = {}

    resp.forEach(function(d) {
      var bucket = moment.unix(d.date).utc()
      var price = Number(d.price)
      var amount = Number(d.amount)

      bucket = bucket.startOf('minute')
      .subtract(bucket.minutes() % 5, 'minute')
      .format('YYYY-MM-DDTHH:mm:ss[Z]')

      if (!buckets[bucket]) {
        buckets[bucket] = {
          base_volume: 0,
          counter_volume: 0,
          count: 0,
          buy_volume: 0,
          sell_volume: 0,
          buy_count: 0,
          sell_count: 0,
          open: price,
          high: price,
          low: price,
          close: price
        }
      }

      if (price > buckets[bucket].high) {
        buckets[bucket].high = price
      }

      if (price < buckets[bucket].low) {
        buckets[bucket].low = price
      }


      buckets[bucket].close = price
      buckets[bucket].base_volume += amount
      buckets[bucket].counter_volume += amount * price
      buckets[bucket].count++

      if (d.type === '1') {
        buckets[bucket].sell_volume += amount
        buckets[bucket].sell_count++
      } else {
        buckets[bucket].buy_volume += amount
        buckets[bucket].buy_count++
      }
    })

    var results = Object.keys(buckets).map(function(key) {
      var row = buckets[key]
      row.source = 'bitstamp.net'
      row.interval = '5minute'
      row.base_currency = 'XRP'
      row.counter_currency = currency
      row.date = key
      row.vwap = row.counter_volume / row.base_volume
      row.vwap = round(row.vwap, 6)
      return row
    })

    console.log('bitstamp.net', results.length)
    return results
  })
}

/**
 * getBTC38
 */

function getBTC38(currency) {


  // hourly: getTradeTimeLine
  // 'http://www.btc38.com/trade/getTrade5minLine.php' +
  // '?coinname=XRP&mk_type=' + currency
  var url = 'http://k.sosobtc.com/data/period'
  var symbol = 'btc38xrp' + (currency === 'BTC' ?
      'btcbtc' : currency.toLowerCase())

  return request({
    url: url,
    json: true,
    qs: {
      symbol: symbol,
      step: 300
    }
  }).then(function(resp) {
    var results = []

    resp.forEach(function(r) {
      if (!r[5]) {
        return
      }

      results.push({
        date: smoment(r[0]).format(),
        source: 'btc38.com',
        interval: '5minute',
        base_currency: 'XRP',
        counter_currency: currency,
        base_volume: r[5],
        open: r[1],
        high: r[2],
        low: r[3],
        close: r[4]
      })
    })

    console.log('btc38.com', currency, results.length)
    return results
  })
}

/**
 * getPoloniex
 */

function getPoloniex(currency) {

  var start = smoment()
  var end = smoment()
  var c = currency

  start.moment.subtract(1, 'days')
  var url = 'https://poloniex.com/public?' +
    'command=returnChartData&currencyPair=' +
     currency + '_XRP&period=300' +
    '&start=' + start.moment.unix() +
    '&end=' + end.moment.unix()

  if (c === 'USDT') {
    c = 'USD'
  }

  return request({
    url: url,
    json: true
  }).then(function(resp) {

    var results = []
    resp.forEach(function(r) {

      // only include intervals with a trade
      if (r.volume === 0) {
        return
      }

      results.push({
        date: smoment(r.date).format(),
        source: 'poloniex.com',
        interval: '5minute',
        base_currency: 'XRP',
        counter_currency: c,
        base_volume: r.quoteVolume,
        counter_volume: r.volume,
        open: r.open,
        high: r.high,
        low: r.low,
        close: r.close,
        vwap: r.weightedAverage
      })
    })

    console.log('poloniex.com', c, results.length)
    return results
  })
}

/**
 * getJubi
 */

function getJubi() {
  var url = 'http://www.jubi.com/coin/xrp/k.js'

  return request({
    url: url
  }).then(function(resp) {

    var results = []
    var data = resp.trim().substr(6, resp.length - 8)

    data = data.replace(/(['"])?([a-z0-9A-Z_]+)(['"])?:/g, '"$2": ')
    data = JSON.parse(data)

    data.time_line['5m'].forEach(function(r) {
      results.push({
        date: smoment(r[0] / 1000).format(),
        source: 'jubi.com',
        interval: '5minute',
        base_currency: 'XRP',
        counter_currency: 'CNY',
        base_volume: r[1],
        open: r[2],
        high: r[3],
        low: r[4],
        close: r[5]
      })
    })

    console.log('jubi.com', results.length)
    return results
  })
}

/**
 * getKraken
 */

function getKraken() {
  var url = 'https://api.kraken.com/0/public/OHLC'
  var pair = 'XXRPXXBT'

  return request({
    url: url,
    json: true,
    qs: {
      pair: pair,
      interval: 5
    }
  }).then(function(resp) {
    var results = []

    resp.result[pair].forEach(function(r) {

      // only include intervals with a trade
      if (r[7] === 0) {
        return
      }

      var vwap = 1 / Number(r[5])

      results.push({
        date: smoment(r[0]).format(),
        source: 'kraken.com',
        interval: '5minute',
        base_currency: 'XRP',
        counter_currency: 'BTC',
        base_volume: Number(r[6]),
        counter_volume: Number(r[6]) / vwap,
        open: round(Number(r[1]), 6),
        high: round(Number(r[2]), 6),
        low: round(Number(r[3]), 6),
        close: round(Number(r[4]), 6),
        vwap: round(1 / vwap, 6),
        count: r[7]
      })
    })

    console.log('kraken.com', results.length)
    return results
  })
}

/**
 * getKraken
 */

function getBittrex() {
  var url = 'https://bittrex.com/api/v1.1/public/getmarkethistory'
  var pair = 'BTC-XRP'

  return request({
    url: url,
    json: true,
    qs: {
      market: pair
    }
  }).then(function(resp) {
    var buckets = {}

    var data = {
      base: 0,
      counter: 0,
      count: 0
    }

    resp.result.forEach(function(d) {
      var bucket = moment.utc(d.TimeStamp)


      data.base += d.Quantity
      data.counter += d.Total
      data.count++


      bucket = bucket.startOf('minute')
      .subtract(bucket.minutes() % 5, 'minute')
      .format('YYYY-MM-DDTHH:mm:ss[Z]')

      if (!buckets[bucket]) {
        buckets[bucket] = {
          base_volume: 0,
          counter_volume: 0,
          count: 0
        }
      }

      buckets[bucket].base_volume += d.Quantity
      buckets[bucket].counter_volume += d.Total
      buckets[bucket].count++
    })

    var results = Object.keys(buckets).map(function(key) {
      var row = buckets[key]
      row.source = 'bittrex.com'
      row.interval = '5minute'
      row.base_currency = 'XRP'
      row.counter_currency = 'BTC'
      row.date = key
      row.vwap = row.counter_volume / row.base_volume
      row.vwap = round(row.vwap, 6)
      return row
    })

    // drop the oldest row,
    // since we dont know if
    // all exchanges were represented
    results.pop()
    console.log('bittrex.com', results.length)
    return results
  })
}

/**
 * reduce
 */

function reduce(data) {
  var reduced = {
    base_volume: 0,
    counter_volume: 0,
    count: 0
  }

  data.forEach(function(d) {
    reduced.base_volume += Number(d.base_volume || 0)
    reduced.counter_volume += Number(d.counter_volume || 0)
    reduced.count += Number(d.count || 0)
  })

  if (!reduced.count) {
    delete reduced.count
  }

  if (reduced.counter_volume) {
    reduced.vwap = reduced.counter_volume / reduced.base_volume
    reduced.vwap = round(reduced.vwap, 6)

  } else {
    delete reduced.counter_volume
  }

  return reduced
}

/**
 * save
 */

function save(data) {
  var rows = {}
  data.forEach(function(rowset) {
    if (!rowset) {
      return
    }

    rowset.forEach(function(r) {
      var date = smoment(r.date)
      var rowkey = r.source + '|' +
        r.base_currency + '|' +
        r.counter_currency + '|' +
        r.interval + '|' +
        date.hbaseFormatStartRow()

      rows[rowkey] = {
        'f:date': r.date,
        'f:source': r.source,
        'f:interval': r.interval,
        'f:base_currency': r.base_currency,
        'f:counter_currency': r.counter_currency,
        base_volume: r.base_volume,
        counter_volume: r.counter_volume,
        open: r.open,
        high: r.high,
        low: r.low,
        close: r.close,
        vwap: r.vwap,
        count: r.count,
        buy_count: r.buy_count,
        sell_count: r.sell_count,
        buy_volume: r.buy_volume,
        sell_volume: r.sell_volume
      }
    })
  })

  console.log('saving ' + Object.keys(rows).length + ' rows')
  return hbase.putRows({
    table: table,
    rows: rows
  })
}

/**
 * savePeriod
 */

function savePeriod(period, increment) {
  var markets = [
    'bitstamp.net|XRP|USD',
    'bitstamp.net|XRP|EUR',
    'bittrex.com|XRP|BTC',
    'poloniex.com|XRP|BTC',
    'poloniex.com|XRP|USD',
    'kraken.com|XRP|BTC',
    'btc38.com|XRP|CNY',
    'btc38.com|XRP|BTC',
    'jubi.com|XRP|CNY'
  ]

  var tasks = []
  var end = smoment()
  var start = smoment()
  var label = (increment || '') + period

  // save single market
  function saveMarket(m) {
    return new Promise(function(resolve, reject) {
      var startRow = m + '|5minute|' + start.hbaseFormatStartRow()
      var stopRow = m + '|5minute|' + end.hbaseFormatStopRow()

      hbase.getScan({
        table: table,
        startRow: startRow,
        stopRow: stopRow
      }, function(err, resp) {

        if (err) {
          reject(err)
        } else {
          var d = reduce(resp)
          var parts = m.split('|')
          d.source = parts[0]
          d.base_currency = parts[1]
          d.counter_currency = parts[2]
          resolve(d)
        }
      })
    })
  }

  start.moment.subtract(increment || 1, period)

  markets.forEach(function(m) {
    tasks.push(saveMarket(m))
  })

  return Promise.all(tasks)
  .then(function(components) {
    var result = {
      components: components,
      period: label,
      total: 0,
      date: end.format()
    }

    components.forEach(function(d) {
      result.total += d.base_volume
    })

    console.log('saving: ' + label +
                ' ' + result.total + ' XRP')
    return hbase.putRow({
      table: 'agg_metrics',
      rowkey: 'trade_volume|external|live|' + label,
      columns: result
    })
  })
}

Promise.all([
  getBitstamp('USD'),
  getBitstamp('EUR'),
  getBTC38('CNY'),
  getBTC38('BTC'),
  getPoloniex('BTC'),
  getPoloniex('USDT'),
  getJubi(),
  getKraken(),
  getBittrex()
])
.then(save)
.then(savePeriod.bind(this, 'hour', 1))
.then(savePeriod.bind(this, 'day', 1))
.then(savePeriod.bind(this, 'day', 3))
.then(savePeriod.bind(this, 'day', 7))
.then(savePeriod.bind(this, 'day', 30))
.then(function() {
  console.log('success')
  process.exit(0)
})
.catch(function(e) {
  console.log('error', e, e.stack)
  process.exit(1)
})
