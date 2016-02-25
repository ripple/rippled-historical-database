var async = require('async');
var Promise = require('bluebird');
var smoment = require('../lib/smoment');
var config = require('../config/import.config');
var Hbase = require('../lib/hbase/hbase-client');
var hbaseOptions = config.get('hbase');
var hbase;

var hbaseOptions2 = JSON.parse(JSON.stringify(hbaseOptions));
hbaseOptions2.prefix = 'stage_';
var hbase2 = new Hbase(hbaseOptions2);

var intervals = [
  'day',
  'week',
  'month'
];

var options = {
  interval: config.get('interval'),
  start: config.get('start'),
  end: config.get('end'),
  save: config.get('save'),
  top: config.get('top')
};

var marketPairs = [
  {
    // Bitstamp USD market
    base: {currency: 'USD', issuer: 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B'},
    counter: {currency: 'XRP'}
  },
  {
    // Bitstamp BTC market
    base: {currency: 'BTC', issuer: 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B'},
    counter: {currency: 'XRP'}
  },
  {
    // Gatehub USD market
    base: {currency: 'USD', issuer: 'rhub8VRN55s94qWKDv6jmDy1pUykJzF3wq'},
    counter: {currency: 'XRP'}
  },
  {
    // RippleCN CNY market
    base: {currency: 'CNY', issuer: 'rnuF96W4SZoCJmbHYBFoJZpR8eCaxNvekK'},
    counter: {currency: 'XRP'}
  },
  {
    // RippleChina CNY market
    base: {currency: 'CNY', issuer: 'razqQKzJRdB4UxFPWf5NEpEG3WMkmwgcXA'},
    counter: {currency: 'XRP'}
  },
  {
    // RippleFox CNY market
    base: {currency: 'CNY', issuer: 'rKiCet8SdvWxPXnAgYarFUXMh1zCPz432Y'},
    counter: {currency: 'XRP'}
  },
  {
    // SnapSwap EUR market
    base: {currency: 'EUR', issuer: 'rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q'},
    counter: {currency: 'XRP'}
  },
  {
    // SnapSwap BTC market
    base: {currency:'BTC', issuer: 'rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q'},
    counter: {currency:'XRP'}
  },
  {
    // TokyoJPY JPY
    base: {currency:'JPY', issuer: 'r94s8px6kSw1uZ1MV98dhSRTvc6VMPoPcN'},
    counter: {currency:'XRP'}
  },
  {
    // Digital Gate Japan JPY
    base: {currency:'JPY', issuer: 'rJRi8WW24gt9X85PHAxfWNPCizMMhqUQwg'},
    counter: {currency:'XRP'}
  },
  {
    // Ripple Exchange Tokyo JPY
    base: {currency:'JPY', issuer: 'r9ZFPSb1TFdnJwbTMYHvVwFK1bQPUCVNfJ'},
    counter: {currency:'XRP'}
  },
  {
    // Mr Ripple JPY
    base: {currency:'JPY', issuer: 'rB3gZey7VWHYRqJHLoHDEJXJ2pEPNieKiS'},
    counter: {currency:'XRP'}
  },
  {
    // Ripple Fox STR
    base: {currency:'STR', issuer: 'rKiCet8SdvWxPXnAgYarFUXMh1zCPz432Y'},
    counter: {currency:'XRP'}
  },
  {
    // Ripple Fox FMM
    base: {currency:'FMM', issuer: 'rKiCet8SdvWxPXnAgYarFUXMh1zCPz432Y'},
    counter: {currency:'XRP'}
  },
  {
    // Bitso MXN
    base: {currency:'MXN', issuer: 'rG6FZ31hDHN1K5Dkbma3PSB5uVCuVVRzfn'},
    counter: {currency:'XRP'}
  },
  {
    // Bitso BTC
    base: {currency:'BTC', issuer: 'rG6FZ31hDHN1K5Dkbma3PSB5uVCuVVRzfn'},
    counter: {currency:'XRP'}
  },
  {
    // The Rock BTC
    base: {currency:'BTC', issuer: 'rLEsXccBGNR3UPuPu2hUXPjziKC3qKSBun'},
    counter: {currency:'XRP'}
  },
  {
    // EXRP KRW
    base: {currency:'KRW', issuer: 'rPxU6acYni7FcXzPCMeaPSwKcuS2GTtNVN'},
    counter: {currency:'XRP'}
  },
  {
    // Bitstamp BTC/USD
    base    : {currency: 'BTC', issuer: 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B'},
    counter : {currency: 'USD', issuer: 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B'},
  },
  {
    // Bitstamp BTC/ Snapswap BTC
    base    : {currency: 'BTC', issuer: 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B'},
    counter : {currency: 'BTC', issuer: 'rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q'},
  },
  {
    // Bitstamp USD/ rippleCN CNY
    base    : {currency: 'CNY', issuer: 'rnuF96W4SZoCJmbHYBFoJZpR8eCaxNvekK'},
    counter : {currency: 'USD', issuer: 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B'}
  },
  {
    // Bitstamp USD/ rippleChina CNY
    base    : {currency: 'CNY', issuer: 'razqQKzJRdB4UxFPWf5NEpEG3WMkmwgcXA'},
    counter : {currency: 'USD', issuer: 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B'}
  },
  {
    // Bitstamp USD/ rippleFox CNY
    base    : {currency: 'CNY', issuer: 'rKiCet8SdvWxPXnAgYarFUXMh1zCPz432Y'},
    counter : {currency: 'USD', issuer: 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B'}
  },
  {
    // Snapswap USD/ rippleFox CNY
    base    : {currency: 'CNY', issuer: 'rKiCet8SdvWxPXnAgYarFUXMh1zCPz432Y'},
    counter : {currency: 'FMM', issuer: 'rKiCet8SdvWxPXnAgYarFUXMh1zCPz432Y'}
  },
  {
    // TokyoJPY JPY/ rippleFox CNY
    base    : {currency: 'JPY', issuer: 'r94s8px6kSw1uZ1MV98dhSRTvc6VMPoPcN'},
    counter : {currency: 'CNY', issuer: 'rKiCet8SdvWxPXnAgYarFUXMh1zCPz432Y'}
  },
  {
    // TokyoJPY JPY/ Snapswap BTC
    base    : {currency: 'JPY', issuer: 'r94s8px6kSw1uZ1MV98dhSRTvc6VMPoPcN'},
    counter : {currency: 'BTC', issuer: 'rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q'}
  },
  {
    // TokyoJPY JPY/ Bitstamp USD
    base    : {currency: 'JPY', issuer: 'r94s8px6kSw1uZ1MV98dhSRTvc6VMPoPcN'},
    counter : {currency: 'USD', issuer: 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B'}
  }
];


if (!options.save) {
  hbaseOptions.logLevel = 1;
}

// long timeout needed
// for month interval
hbaseOptions.timeout = 120000;
hbase = new Hbase(hbaseOptions);

// run aggregations
aggregateTradeVolume(options);

/**
 * aggregateTradeVolume
 */

function aggregateTradeVolume(params) {

  var list = [];
  var start;
  var end;
  var interval;
  var live;

  if (!params) {
    params = {};
  }

  interval = (params.interval || '').toLowerCase();
  start = smoment(params.start);
  end = smoment(params.end);
  start.granularity = 'second';

  // invalid start date
  // or end date
  if (!start || !end) {
    console.log('invalid start or end time');
    process.exit(1);
  }

  // invalid interval
  if (interval && intervals.indexOf(interval) === -1) {
    console.log('invalid interval:', interval);
    process.exit(1);

  // history, multiple
  } else if (interval && params.end) {
    start.moment.startOf(interval === 'week' ? 'isoWeek' : interval);
    while (end.moment.diff(start.moment) >= 0) {
      list.push({
        interval: interval,
        start: smoment(start.format())
      });

      start.moment.add(1, interval);
    }

  // history, single
  } else if (interval) {
    start.moment.startOf(interval === 'week' ? 'isoWeek' : interval);
    list.push({
      interval: interval,
      start: start
    });

  // live (24hrs)
  } else {
    start = smoment();
    start.moment.subtract(24, 'hours');
    list.push({
      start: start,
      end: smoment(),
      live: true
    });
  }

  // aggregate each in series
  async.mapSeries(list, handleAggregation, function(err, resp) {

    if (err) {
      console.log(err);

    // print to stdout
    } else if (!options.save) {
      if (resp.length === 1) {
        resp = resp[0];
      }

      process.stdout.write(JSON.stringify(resp, null, 2)+'\n');
    }

    process.exit(err ? 1 : 0);
  });
}

/**
 * handleAggregation
 */

function handleAggregation (params, done) {

  getMarkets()
  .then(getVolumes)
  .then(getRates)
  .then(normalize)
  .then(save)
  .nodeify(done);


  /**
   * getMarkets
   * get markets either from the list here or
   * hbase top markets
   */

  function getMarkets() {
    return new Promise(function(resolve, reject) {
      var date;
      var max;

      if (options.top) {

        if (!params.live) {
          max = smoment();
          max.moment.subtract(2, 'days').startOf('day');

          // get the date at the end
          // of the provided interval
          date = smoment(params.start);
          date.moment.startOf(params.interval);

          // use max if the date
          // provided is later than that
          if (date.moment.diff(max.moment) > 0) {
            date = max;
          }
        }

        hbase.getTopMarkets({date: date}, function(err, markets) {

          if (err) {
            reject(err);

          // format results
          } else {
            markets.forEach(function(market) {
              market.base = {
                currency: market.base_currency,
                issuer: market.base_issuer
              };
              market.counter = {
                currency: market.counter_currency,
                issuer: market.counter_issuer
              };
            });
            resolve(markets);
          }
        });

      } else {
        resolve(marketPairs);
      }
    });
  }

  /**
   * getVolumes
   * get trade volume for each market
   */

  function getVolumes(markets) {

    markets.forEach(function(market) {
      var swap;

      if (market.base.currency === 'XRP') {
        swap = market.base;
        market.base = market.counter;
        market.counter = swap;
      }
    });

    return Promise.map(markets, function(market) {
      return new Promise(function(resolve, reject) {
        var start = smoment(params.start);
        var end = smoment(params.start);

        end.moment.add(1, params.interval || 'day').subtract(1, 'second');

        hbase.getExchanges({
          base: market.base,
          counter: market.counter,
          start: start,
          end: end,
          limit: Infinity,
          reduce: true
        }, function(err, resp) {
          var data = {};

          if (err) {
            reject(err);
            return;
          }

          resp = resp.reduced;

          data.count = resp.count;
          data.rate = resp.vwap;
          data.amount = resp.base_volume;
          data.base = market.base;
          data.counter = market.counter;

          if (data.counter.currency === 'XRP') {
            data.converted_amount = resp.counter_volume;
            data.rate = resp.vwap ? 1 / resp.vwap : 0;
          }

          resolve(data);
        });
      });
    });
  }

  /**
   * getRates
   * get XRP conversion rates from the results
   */

  function getRates(markets) {
    var data = {
      markets: markets,
      rates: {}
    };

    data.markets.forEach(function(market) {
      if (market.counter.currency === 'XRP') {
        data.rates[market.base.currency + '.' + market.base.issuer] = market.rate;
      }
    });

    return data;
  }

  /**
   * normalize
   * convert the amounts to XRP using the rates
   * found.  If we don't have a rate, it wont be included
   */

  function normalize(data) {
    var total = 0;
    var count = 0;

    data.markets.forEach(function(market) {
      var base = market.base.currency + '.' + market.base.issuer;
      var counter = market.counter.currency + '.' + market.counter.issuer;
      var swap

      // no trades or already determined
      if (!market.count || market.converted_amount) {
        return;

      } else if (data.rates[base]) {
        market.converted_amount = market.amount / data.rates[base];
        market.rate /= data.rates[base];

      } else if (data.rates[counter]) {
        swap = market.base;
        market.base = market.counter;
        market.counter = swap;
        market.amount = market.rate * market.amount;
        market.converted_amount = market.amount / data.rates[counter];
        market.rate /= data.rates[counter];

      } else {
        console.log('no rate for:', base, counter);
      }
    });

    data.markets = data.markets.filter(function(market) {
      return market.count && market.converted_amount;
    });

    data.markets.sort(function(a, b) {
      return b.converted_amount - a.converted_amount;
    });

    data.markets.forEach(function(market) {
      total += market.converted_amount;
      count += market.count;
    });

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

      var table = 'agg_metrics';
      var rowkey = 'trade_volume|';

      if (options.save) {
        rowkey += params.live ? 'live' : params.interval + '|' + params.start.hbaseFormatStartRow();

        console.log('saving:', table, rowkey);
        return hbase2.putRow(table, rowkey, result)
        .nodeify(function(err, resp) {
          if (err) {
            reject(err);
          } else {
            delete result.components;
            console.log('saved:', table, rowkey);
            console.log(result);
            resolve(rowkey);
          }
        });

      } else {
        resolve(result);
      }
    });
  }
}
