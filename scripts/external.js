/* eslint no-unused-vars: 1 */
'use strict'

var request = require('request-promise')
var smoment = require('../lib/smoment')
var moment = require('moment')
var hbase = require('../lib/hbase')
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

    // drop the oldest row,
    // since we dont know if
    // all exchanges were represented
    results.pop()
    console.log('bitstamp.net', currency, results.length)
    return results
  })
  .catch(function(e) {
    console.log('bitstamp error:', e)
  })
}

/**
 * getBithumb
 */

function getBithumb() {

  var url = 'https://api.bithumb.com/public/recent_transactions/xrp'


  return request({
    url: url,
    json: true,
    qs: {
      count: 100
    }
  }).then(function(resp) {
    var buckets = {}

    resp.data.forEach(function(d) {
      var bucket = moment.utc(d.transaction_date, 'YYYY-MM-DD HH:mm:ss').utcOffset('-0900')
      var price = Number(d.price)
      var amount = Number(d.units_traded)

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

      if (d.type === 'bid') {
        buckets[bucket].sell_volume += amount
        buckets[bucket].sell_count++
      } else {
        buckets[bucket].buy_volume += amount
        buckets[bucket].buy_count++
      }
    })

    var results = Object.keys(buckets).map(function(key) {
      var row = buckets[key]
      row.source = 'bithumb.com'
      row.interval = '5minute'
      row.base_currency = 'XRP'
      row.counter_currency = 'KRW'
      row.date = key
      row.vwap = row.counter_volume / row.base_volume
      row.vwap = round(row.vwap, 6)
      return row
    })

    // drop the oldest row,
    // since we dont know if
    // all exchanges were represented
    results.pop()
    console.log('bithumb.com', results.length)
    return results
  })
  .catch(function(e) {
    console.log('bithumb error:', e)
  })
}

/**
 * getBtcxIndia
 */

function getBtcxIndia() {

  var url = 'https://api.btcxindia.com/trades'

  return request({
    url: url,
    json: true
  })
  .then(function(resp) {
    var buckets = {}

    resp.data.transactions.forEach(function(d) {
      var bucket = moment.utc(d.time, 'YYYY-MM-DD HH:mm:ss')
      var price = Number(d.price)
      var amount = Number(d.volume)

      bucket = bucket.startOf('minute')
      .subtract(bucket.minutes() % 5, 'minute')
      .format('YYYY-MM-DDTHH:mm:ss[Z]')

      if (!buckets[bucket]) {
        buckets[bucket] = {
          base_volume: 0,
          counter_volume: 0,
          count: 0,
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
    })

    var results = Object.keys(buckets).map(function(key) {
      var row = buckets[key]
      row.source = 'btcxindia.com'
      row.interval = '5minute'
      row.base_currency = 'XRP'
      row.counter_currency = 'INR'
      row.date = key
      row.vwap = row.counter_volume / row.base_volume
      row.vwap = round(row.vwap, 6)
      return row
    })

    // drop the oldest row,
    // since we dont know if
    // all exchanges were represented
    results.pop()
    console.log(results)
    console.log('btcxindia.com', results.length)
    return results
  })
  .catch(function(e) {
    console.log('btcxindia error:', e)
  })
}

/**
 * getBitbank
 */

function getBitbank() {

  var url = 'https://public.bitbank.cc/xrp_jpy/transactions'

  return request({
    url: url,
    json: true
  })
  .then(function(resp) {
    var buckets = {}

    resp.data.transactions.forEach(function(d) {
      var bucket = moment(d.executed_at).utc()
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

      if (d.side === 'sell') {
        buckets[bucket].sell_volume += amount
        buckets[bucket].sell_count++
      } else {
        buckets[bucket].buy_volume += amount
        buckets[bucket].buy_count++
      }
    })

    var results = Object.keys(buckets).map(function(key) {
      var row = buckets[key]
      row.source = 'bitbank.cc'
      row.interval = '5minute'
      row.base_currency = 'XRP'
      row.counter_currency = 'JPY'
      row.date = key
      row.vwap = row.counter_volume / row.base_volume
      row.vwap = round(row.vwap, 6)
      return row
    })

    // drop the oldest row,
    // since we dont know if
    // all exchanges were represented
    results.pop()
    console.log('bitbank.cc', results.length)
    return results
  })
  .catch(function(e) {
    console.log('bitbank error:', e)
  })
}

/**
 * getBitfinex
 */

function getBitfinex(currency) {

  var pair = ('xrp' + currency).toLowerCase()
  var url = 'https://api.bitfinex.com/v1/trades/'


  return request({
    url: url + pair,
    json: true
  }).then(function(resp) {
    var buckets = {}

    resp.forEach(function(d) {
      var bucket = moment.unix(d.timestamp).utc()
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

      if (d.type === 'sell') {
        buckets[bucket].sell_volume += amount
        buckets[bucket].sell_count++
      } else {
        buckets[bucket].buy_volume += amount
        buckets[bucket].buy_count++
      }
    })

    var results = Object.keys(buckets).map(function(key) {
      var row = buckets[key]
      row.source = 'bitfinex.com'
      row.interval = '5minute'
      row.base_currency = 'XRP'
      row.counter_currency = currency
      row.date = key
      row.vwap = row.counter_volume / row.base_volume
      row.vwap = round(row.vwap, 6)
      return row
    })

    // drop the oldest row,
    // since we dont know if
    // all exchanges were represented
    results.pop()
    console.log('bitfinex.com', currency, results.length)
    return results
  })
  .catch(function(e) {
    console.log('bitfinex error:', e)
  })
}

/**
 * getBitso
 */

function getBitso(currency) {

  var pair = ('xrp_' + currency).toLowerCase()
  var url = 'https://api.bitso.com/v3/trades'


  return request({
    url: url,
    json: true,
    qs: {
      book: pair,
      limit: 100
    }
  }).then(function(resp) {
    var buckets = {}

    resp.payload.forEach(function(d) {
      var bucket = moment(d.created_at).utc()
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

      if (d.maker_side === 'buy') {
        buckets[bucket].sell_volume += amount
        buckets[bucket].sell_count++
      } else {
        buckets[bucket].buy_volume += amount
        buckets[bucket].buy_count++
      }
    })

    var results = Object.keys(buckets).map(function(key) {
      var row = buckets[key]
      row.source = 'bitso.com'
      row.interval = '5minute'
      row.base_currency = 'XRP'
      row.counter_currency = currency
      row.date = key
      row.vwap = row.counter_volume / row.base_volume
      row.vwap = round(row.vwap, 6)
      return row
    })

    // drop the oldest row,
    // since we dont know if
    // all exchanges were represented
    results.pop()
    console.log('bitso.com', currency, results.length)
    return results
  })
  .catch(function(e) {
    console.log('bitso error:', e)
  })
}

/**
 * getCoinone
 */

function getCoinone() {

  var url = 'https://api.coinone.co.kr/trades'

  return request({
    url: url,
    json: true,
    qs: {
      currency: 'xrp',
      period: 'hour'
    }
  }).then(function(resp) {

    var buckets = {}

    resp.completeOrders.forEach(function(d) {
      var bucket = moment.unix(d.timestamp).utc()
      var price = Number(d.price)
      var amount = Number(d.qty)

      bucket = bucket.startOf('minute')
      .subtract(bucket.minutes() % 5, 'minute')
      .format('YYYY-MM-DDTHH:mm:ss[Z]')

      if (!buckets[bucket]) {
        buckets[bucket] = {
          base_volume: 0,
          counter_volume: 0,
          count: 0,
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
    })

    var results = Object.keys(buckets).map(function(key) {
      var row = buckets[key]
      row.source = 'coinone.co.kr'
      row.interval = '5minute'
      row.base_currency = 'XRP'
      row.counter_currency = 'KRW'
      row.date = key
      row.vwap = row.counter_volume / row.base_volume
      row.vwap = round(row.vwap, 6)
      return row
    })

    // drop the oldest row,
    // since we dont know if
    // all exchanges were represented
    results.pop()
    console.log('coinone.co.kr', results.length)
    return results
  })
  .catch(function(e) {
    console.log('coinone error:', e)
  })
}

/**
 * getBTC38
 */

function getCoincheck() {

  var url = 'https://coincheck.com/exchange/candle_rates'

  return request({
    url: url,
    json: true,
    qs: {
      limit: 288,
      market: 'coincheck',
      pair: 'xrp_jpy',
      unit: 300,
      v2: true
    }
  }).then(function(resp) {

    var results = []

    resp.forEach(function(r) {
      if (!r[5]) {
        return
      }

      results.push({
        date: smoment(r[0]).format(),
        source: 'coincheck.com',
        interval: '5minute',
        base_currency: 'XRP',
        counter_currency: 'JPY',
        base_volume: r[5],
        open: r[1],
        high: r[2],
        low: r[3],
        close: r[4]
      })
    })

    console.log('coincheck.com', 'JPY', results.length)
    return results
  })
  .catch(function(e) {
    console.log('coincheck error:', e)
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
  .catch(function(e) {
    console.log('btc38.com error:', currency, e)
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
  .catch(function(e) {
    console.log('polniex.com error:', c, e)
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

  .catch(function(e) {
    console.log('jubiu error:', e)
  })
}

/**
 * getKraken
 */

function getKraken(currency) {

  var url = 'https://api.kraken.com/0/public/OHLC'
  var pair = 'XXRP' +
    (currency === 'BTC' ? 'XXBT' : 'Z' + currency)

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
        counter_currency: currency,
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

    console.log('kraken.com', currency, results.length)
    return results
  })
  .catch(function(e) {
    console.log('kraken error:', e)
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
  .catch(function(e) {
    console.log('bittrex error:', e)
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
  // console.log(data)
  // process.exit()

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
    // 'coincheck.com|XRP|JPY',
    // 'btcxindia.com|XRP|KRW',
    'bithumb.com|XRP|KRW',
    'bitbank.cc|XRP|JPY',
    'coinone.co.kr|XRP|KRW',
    'bitfinex.com|XRP|USD',
    'bitfinex.com|XRP|BTC',
    'bitso.com|XRP|MXN',
    'bitso.com|XRP|BTC',
    'bitstamp.net|XRP|BTC',
    'bitstamp.net|XRP|USD',
    'bitstamp.net|XRP|EUR',
    'bittrex.com|XRP|BTC',
    'poloniex.com|XRP|BTC',
    'poloniex.com|XRP|USD',
    'kraken.com|XRP|BTC',
    'kraken.com|XRP|USD',
    'kraken.com|XRP|EUR',
    'kraken.com|XRP|CAD',
    'kraken.com|XRP|JPY',
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

        } else if (!resp.length) {
          console.log(m + ': no data')
          resolve()

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
      components: components.filter(function(d) {
        return Boolean(d)
      }),
      period: label,
      total: 0,
      date: end.format()
    }

    result.components.forEach(function(d) {
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
  // getCoincheck(),
  // getBtcxIndia(),
  getBithumb(),
  getBitbank(),
  getBitfinex('USD'),
  getBitfinex('BTC'),
  getBitso('MXN'),
  getBitso('BTC'),
  getKraken('BTC'),
  getKraken('USD'),
  getKraken('EUR'),
  getKraken('CAD'),
  getKraken('JPY'),
  getCoinone(),
  getBitstamp('BTC'),
  getBitstamp('USD'),
  getBitstamp('EUR'),
  getBTC38('CNY'),
  getBTC38('BTC'),
  getPoloniex('BTC'),
  getPoloniex('USDT'),
  getJubi(),
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
