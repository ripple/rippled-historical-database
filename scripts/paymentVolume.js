'use strict';

var async = require('async');
var Promise = require('bluebird');
var utils = require('../lib/utils');
var smoment = require('../lib/smoment');
var config = require('../config/import.config');
var Hbase = require('../lib/hbase/hbase-client');
var hbaseOptions = config.get('hbase');
var hbase;

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

if (!options.save) {
  hbaseOptions.logLevel = 1;
}

var conversionPairs = [];
var currencies = [
  {currency: 'USD', issuer: 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B'},  //Bitstamp USD
  {currency: 'USD', issuer: 'rhub8VRN55s94qWKDv6jmDy1pUykJzF3wq'}, //Gatehub USD

  {currency: 'BTC', issuer: 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B'},  //Bitstamp BTC
  {currency: 'BTC', issuer: 'rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q'}, //Snapswap BTC
  {currency: 'BTC', issuer: 'rG6FZ31hDHN1K5Dkbma3PSB5uVCuVVRzfn'}, //Bitso BTC
  {currency: 'BTC', issuer: 'rLEsXccBGNR3UPuPu2hUXPjziKC3qKSBun'}, //The Rock BTC

  {currency: 'EUR', issuer: 'rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q'}, //Snapswap EUR
  {currency: 'EUR', issuer: 'rhub8VRN55s94qWKDv6jmDy1pUykJzF3wq'}, //Gatehub USD

  {currency: 'CNY', issuer: 'rnuF96W4SZoCJmbHYBFoJZpR8eCaxNvekK'}, //RippleCN CNY
  {currency: 'CNY', issuer: 'razqQKzJRdB4UxFPWf5NEpEG3WMkmwgcXA'}, //RippleChina CNY
  {currency: 'CNY', issuer: 'rKiCet8SdvWxPXnAgYarFUXMh1zCPz432Y'}, //RippleFox CNY
  {currency: 'CNY', issuer: 'rM8199qFwspxiWNZRChZdZbGN5WrCepVP1'}, //DotPayco CNY

  {currency: 'JPY', issuer: 'r94s8px6kSw1uZ1MV98dhSRTvc6VMPoPcN'}, //TokyoJPY JPY
  {currency: 'JPY', issuer: 'rJRi8WW24gt9X85PHAxfWNPCizMMhqUQwg'}, //Digital Gate Japan JPY
  {currency: 'JPY', issuer: 'r9ZFPSb1TFdnJwbTMYHvVwFK1bQPUCVNfJ'}, //Ripple Exchange Tokyo JPY
  {currency: 'JPY', issuer: 'rB3gZey7VWHYRqJHLoHDEJXJ2pEPNieKiS'}, //Mr Ripple JPY

  {currency: 'XAU', issuer: 'r9Dr5xwkeLegBeXq6ujinjSBLQzQ1zQGjH'}, //Ripple Singapore XAU
  {currency: 'XAU', issuer: 'rrh7rf1gV2pXAoqA8oYbpHd8TKv5ZQeo67'}, //GBI XAU
  {currency: 'STR', issuer: 'rKiCet8SdvWxPXnAgYarFUXMh1zCPz432Y'}, //Ripple Fox STR
  {currency: 'FMM', issuer: 'rKiCet8SdvWxPXnAgYarFUXMh1zCPz432Y'}, //Ripple Fox FMM
  {currency: 'MXN', issuer: 'rG6FZ31hDHN1K5Dkbma3PSB5uVCuVVRzfn'}, //Bitso MXN
  {currency: 'KRW', issuer: 'rPxU6acYni7FcXzPCMeaPSwKcuS2GTtNVN'},  //EXRP KRW
  {currency: 'XRP'}
];

// populate conversion pairs
currencies.forEach(function(currency) {

  if (currency.currency === 'XRP') {
    return;
  }

  conversionPairs.push({
    base: {currency: 'XRP'},
    counter: currency
  });
});

hbase = new Hbase(hbaseOptions);

aggregatePayments(options);

function aggregatePayments(params) {

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
        start: smoment(start)
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

function handleAggregation(params, done) {

  getCurrencies()
  .then(getVolumes)
  .then(getRates)
  .then(normalize)
  .then(save)
  .nodeify(done);

  function getCurrencies() {
    return new Promise(function(resolve, reject) {
      var date;
      var max;

      if (options.top) {

        if (!params.live) {
          max = smoment();
          max.moment.subtract(2, 'days').startOf('day');

          // get the date at the end
          // of the provided interval
          date = smoment(params.time);
          date.moment.startOf(params.interval);

          // use max if the date
          // provided is later than that
          if (date.moment.diff(max.moment) > 0) {
            date = max;
          }
        }

        hbase.getTopCurrencies({date: date}, function(err, currencyList) {

          if (err) {
            reject(err);

          // no markets found
          } else if (!currencyList.length) {
            reject('no markets found');

          } else {
            currencyList.push({
              currency: 'XRP'
            });

            resolve(currencyList);
          }
        });

      } else {
        resolve(currencies);
      }
    });
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
        };

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
            asyncCallbackPair(err);
            return;
          }

          if (params.interval && data) {
            currency.amount = data.rows && data.rows[0] ? data.rows[0].amount : 0;
            currency.count = data.rows && data.rows[0] ? data.rows[0].count : 0;
          } else if (data) {
            currency.amount = data.amount;
            currency.count = data.count;
          } else {
            currency.amount = 0;
            currency.count = 0;
          }

          asyncCallbackPair(null, currency);
        });
      }, function (err, resp) {
        if (err) {
          reject(err);

        // filter rows with no payments
        } else {
          resolve(resp.filter(function(d) {
            return !!d.count;
          }));
        }
      });
    });
  }


  /**
   * getRates
   */

  function getRates(data) {

    return new Promise(function(resolve, reject) {

      // get exchanges for each pair
      async.map(data, function(d, asyncCallbackPair) {

        if (d.currency === 'XRP') {
          d.rate = 1;
          asyncCallbackPair(null, d);
          return;
        }

        var start = smoment(params.start);
        var end = smoment(params.start);
        start.moment.subtract(14, 'days');

        var options = {
          base: { currency: 'XRP' },
          counter: { currency: d.currency, issuer: d.issuer },
          start: start,
          end: end,
          descending: true
        };

        // use last 50 trades for live
        if (params.live) {
          options.limit = 50;
          options.reduce = true;

        // use daily rate
        // from the previous day
        } else {
          end.moment.subtract(1, 'day');
          options.interval = '1day';
          options.limit = 1;
        }

        hbase.getExchanges(options, function(err, resp) {
          if (err) {
            asyncCallbackPair(err);
            return;
          }

          if (resp && resp.reduced) {
            d.rate = resp.reduced.vwap;
          } else if (resp && resp.rows.length) {
            d.rate = resp.rows[0].vwap;
          } else {
            d.rate = 0;
          }

          asyncCallbackPair(null, d);
        });

      }, function (err, resp) {
        if (err) {
          reject(err);
        } else {
          resolve(resp);
        }
      });
    });
  }

  /**
   * normalize
   */

  function normalize(data) {
    var total = 0;
    var count = 0;

    data.forEach(function(d) {
      d.converted_amount = d.rate ? d.amount / d.rate : 0;
      total += d.converted_amount;
      count += d.count;
    });

    return {
      startTime: smoment(params.start).format(),
      total: total,
      count: count,
      exchange: { currency:'XRP' },
      exchangeRate: 1,
      components: data.filter(function(d) {
        return !!d.converted_amount;
      }).sort(function(a,b) {
        return b.converted_amount - a.converted_amount;
      })
    }
  }

  /**
   * save
   */

  function save(result) {
    return new Promise(function(resolve, reject) {
      if (options.save) {

        var table = 'agg_metrics';
        var rowkey = 'payment_volume|' + (params.live ?
          'live' : options.interval + '|' + smoment(result.startTime).hbaseFormatStartRow());

        hbase.putRow(table, rowkey, result)
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
