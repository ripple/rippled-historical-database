var request = require('request-promise');
var smoment = require('../lib/smoment');
var config = require('../config/import.config');
var Hbase = require('../lib/hbase/hbase-client');
var hbase = new Hbase(config.get('hbase'));

/**
 * getBTC38
 */

function getBTC38(currency) {
  currency = currency.toUpperCase();

  var url = 'http://www.btc38.com/trade/getTradeTimeLine.php' +
    '?coinname=XRP&mk_type=' + currency;

  return request({
    url: url,
    json: true
  }).then(function(resp) {
    var results = [];

    resp.forEach(function(r) {
      results.push({
        date: smoment(r[0] / 1000).format(),
        source: 'btc38.com',
        interval: '1hour',
        base_currency: 'XRP',
        counter_currency: currency,
        base_volume: r[1],
        open: r[2],
        high: r[3],
        low: r[4],
        close: r[5]
      });
    });

    return results;
  });
}

/**
 * getPoloniex
 */

function getPoloniex(currency) {
  var start = smoment();
  var end = smoment();

  currency = currency.toUpperCase();

  start.moment.subtract(5, 'days');
  var url = 'https://poloniex.com/public?' +
    'command=returnChartData&currencyPair=' +
     currency + '_XRP&period=7200' +
    '&start=' + start.moment.unix() +
    '&end=' + end.moment.unix();

  return request({
    url: url,
    json: true
  }).then(function(resp) {

    var results = [];
    resp.forEach(function(r) {
      results.push({
        date: smoment(r.date).format(),
        source: 'poloniex.com',
        interval: '2hour',
        base_currency: 'XRP',
        counter_currency: currency,
        base_volume: r.quoteVolume,
        counter_volume: r.volume,
        open: r.open,
        high: r.high,
        low: r.low,
        close: r.close,
        vwap: r.weightedAverage
      });
    });

    return results;
  });
}

/**
 * getJubi
 */

function getJubi() {
  var url = 'http://www.jubi.com/coin/xrp/k.js';

  return request({
    url: url
  }).then(function(resp) {

    var results = [];
    var data = resp.trim().substr(6, resp.length-8);

    data = data.replace(/(['"])?([a-z0-9A-Z_]+)(['"])?:/g, '"$2": ');
    data = JSON.parse(data);

    data.time_line['1h'].forEach(function(r) {
      results.push({
        date: smoment(r[0] / 1000).format(),
        source: 'jubi.com',
        interval: '1hour',
        base_currency: 'XRP',
        counter_currency: 'CNY',
        base_volume: r[1],
        open: r[2],
        high: r[3],
        low: r[4],
        close: r[5]
      });
    });

    return results;
  });
}

/**
 * getKraken
 */

function getKraken() {
  var url = 'https://api.kraken.com/0/public/OHLC?pair=XXBTXXRP&interval=60';

  return request({
    url: url,
    json: true
  }).then(function(resp) {
    var results = [];

    resp.result.XXBTXXRP.forEach(function(r) {

      // only include intervals with a trade
      if (r[7] === 0) {
        return;
      }

      var vwap = 1 / Number(r[5]);

      results.push({
        date: smoment(r[0]).format(),
        source: 'kraken.com',
        interval: '1hour',
        base_currency: 'XRP',
        counter_currency: 'BTC',
        base_volume: Number(r[6]) / vwap,
        counter_volume: Number(r[6]),
        open: round(1 / Number(r[1]), 6),
        high: round(1 / Number(r[2]), 6),
        low: round(1 / Number(r[3]), 6),
        close: round(1 / Number(r[4]), 6),
        vwap: round(vwap, 6),
        count: r[7]
      });
    });

    return results;
  });
}

/**
 * round
 * round to siginficant digits
 */

function round(n, sig) {
  var mult = Math.pow(10,
      sig - Math.floor(Math.log(n) / Math.LN10) - 1);
  return Math.round(n * mult) / mult;
}

/**
 * save
 */

function save(data) {
  var rows = {};
  data.forEach(function(rowset) {
    rowset.forEach(function(r) {
      var date = smoment(r.date);
      var rowkey = r.source + '|' +
        r.base_currency + '|' +
        r.counter_currency + '|' +
        r.interval + '|' +
        date.hbaseFormatStartRow();

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
        count: r.count
      };
    });
  });

  console.log('saving ' + Object.keys(rows).length + ' rows');
  return hbase.putRows({
    table: 'agg_exchanges_external',
    rows: rows
  });
}

Promise.all([
  getBTC38('CNY'),
  getBTC38('BTC'),
  getPoloniex('BTC'),
  getJubi(),
  getKraken()
])
.then(save)
.then(function() {
  console.log('success');
  process.exit(0);
})
.catch(function(e) {
  console.log('error', e, e.stack);
  process.exit(1);
});
