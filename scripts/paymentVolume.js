'use strict';

var async = require('async');
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
  save: config.get('save')
};

if (!options.save) {
  hbaseOptions.logLevel = 1;
}

var conversionPairs = [];
var currencies = [
  {currency: 'USD', issuer: 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B'},  //Bitstamp USD
  {currency: 'USD', issuer: 'rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q'}, //Snapswap USD
  {currency: 'BTC', issuer: 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B'},  //Bitstamp BTC
  {currency: 'BTC', issuer: 'rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q'}, //Snapswap BTC
  {currency: 'BTC', issuer: 'rG6FZ31hDHN1K5Dkbma3PSB5uVCuVVRzfn'}, //Bitso BTC
  {currency: 'EUR', issuer: 'rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q'}, //Snapswap EUR
  {currency: 'CNY', issuer: 'rnuF96W4SZoCJmbHYBFoJZpR8eCaxNvekK'}, //RippleCN CNY
  {currency: 'CNY', issuer: 'razqQKzJRdB4UxFPWf5NEpEG3WMkmwgcXA'}, //RippleChina CNY
  {currency: 'CNY', issuer: 'rKiCet8SdvWxPXnAgYarFUXMh1zCPz432Y'}, //RippleFox CNY
  {currency: 'CNY', issuer: 'rM8199qFwspxiWNZRChZdZbGN5WrCepVP1'}, //DotPayco CNY
  {currency: 'JPY', issuer: 'r94s8px6kSw1uZ1MV98dhSRTvc6VMPoPcN'}, //TokyoJPY JPY
  {currency: 'JPY', issuer: 'rJRi8WW24gt9X85PHAxfWNPCizMMhqUQwg'}, //Digital Gate Japan JPY
  {currency: 'JPY', issuer: 'r9ZFPSb1TFdnJwbTMYHvVwFK1bQPUCVNfJ'}, //Ripple Exchange Tokyo JPY
  {currency: 'JPY', issuer: 'rB3gZey7VWHYRqJHLoHDEJXJ2pEPNieKiS'}, //Mr Ripple JPY
  {currency: 'STR', issuer: 'rKiCet8SdvWxPXnAgYarFUXMh1zCPz432Y'}, //Ripple Fox STR
  {currency: 'FMM', issuer: 'rKiCet8SdvWxPXnAgYarFUXMh1zCPz432Y'}, //Ripple Fox FMM
  {currency: 'MXN', issuer: 'rG6FZ31hDHN1K5Dkbma3PSB5uVCuVVRzfn'}, //Bitso MXN
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
  var keybase;
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

  // invalid start time
  // or end time
  if (!start || !end) {
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
      process.exit(1);
    }

    // print to stdout
    if (!options.save) {
      if (resp.length === 1) {
        resp = resp[0];
      }

      process.stdout.write(JSON.stringify(resp, null, 2)+'\n');
    }

    process.exit();
  });
}

function handleAggregation(params, done) {

  // prepare results
  var result = {
    startTime: params.start.moment.format(),
    exchange: { currency: 'XRP' },
    exchangeRate: 1,
    total: 0,
    count: 0
  };

  // get payment volume for each currency
  async.map(currencies, function(c, asyncCallbackPair) {

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
  }, normalize);

  /**
   * normalize
   */

  function normalize(err, resp) {

    if (err) {
      done(err);
      return;
    }

    var currencies = resp;

    getExchangeRates(params, function(err, rates) {
      if (err) {
        done(err);
        return;
      }

      // apply rates to each currency
      rates.forEach(function(pair, index) {
        currencies[index].rate = pair.rate || 0;
        currencies[index].convertedAmount = pair.rate ?
          currencies[index].amount / pair.rate : 0;
      });

      // add up the totals
      currencies.forEach(function(currency, index) {

        if (currency.currency == "XRP") {
          currency.rate            = 1; //for XRP
          currency.convertedAmount = currency.amount;
        }

        result.total += currency.convertedAmount;
        result.count += currency.count;
      });

      result.components = currencies;
      handleResult(result);
    });
  }

  function handleResult(result) {

    // save data into hbase
    if (options.save) {

      var rowkey = 'payment_volume|' + (params.live ?
        'live' : options.interval + '|' + smoment(result.startTime).hbaseFormatStartRow());

      console.log('saving row:', rowkey);
      hbase.putRow('agg_metrics', rowkey, result)
      .nodeify(function(err, resp) {

        delete result.components;
        if (!err) {
          console.log(result);
        }

        done(err, result);
      });

    } else {
      done(null, result);
    }
  }
}

/*
 * get exchange rates for the listed currencies
 */

function getExchangeRates(params, callback) {

  var options;

  if (params.live) {
    options = {
      start: smoment('2013-01-01'),
      end: smoment(),
      limit: 50,
      descending: true,
      reduce: true
    }

  //use daily rate
  } else {
    options = {
      descending: false
    };

    if (params.interval === 'week') {
      options.interval = '7day';
    } else if (params.interval) {
      options.interval = '1' + params.interval;
    } else {
      options.reduce = true;
    }
  }

  // get exchanges for each pair
  async.map(conversionPairs, function(assetPair, asyncCallbackPair) {

    options.base = assetPair.base;
    options.counter = assetPair.counter;
    options.start = smoment(params.start);
    options.end = smoment(params.end || params.start);


    hbase.getExchanges(options, function(err, resp) {

      if (err) {
        asyncCallbackPair(err);
        return;
      }

      if (resp && resp.reduced) {
        assetPair.rate = resp.reduced.vwap;
      } else if (resp && resp.rows.length) {
        assetPair.rate = resp.rows[0].vwap;
      } else {
        assetPair.rate = 0;
      }

      asyncCallbackPair(null, assetPair);
    });

  }, function(error, results) {
    if (error) return callback(error);
    return callback(null, results);
  });
}

