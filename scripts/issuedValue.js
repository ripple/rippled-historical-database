var async = require('async');
var Promise = require('bluebird');
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
  start: config.get('start'),
  end: config.get('end'),
  save: config.get('save'),
  top: config.get('top')
};

//all currencies we are going to check
var currencies = [
  {currency: 'USD', issuer: 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B'},  //Bitstamp USD
  {currency: 'USD', issuer: 'rhub8VRN55s94qWKDv6jmDy1pUykJzF3wq'}, //Gatehub USD
  {currency: 'USD', issuer: 'r9Dr5xwkeLegBeXq6ujinjSBLQzQ1zQGjH'}, //Ripple Singapore USD

  {currency: 'BTC', issuer: 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B'},  //Bitstamp BTC
  {currency: 'BTC', issuer: 'rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q'}, //Snapswap BTC
  {currency: 'BTC', issuer: 'rJHygWcTLVpSXkowott6kzgZU6viQSVYM1'}, //Justcoin BTC
  {currency: 'BTC', issuer: 'rG6FZ31hDHN1K5Dkbma3PSB5uVCuVVRzfn'}, //Bitso BTC
  {currency: 'BTC', issuer: 'rLEsXccBGNR3UPuPu2hUXPjziKC3qKSBun'}, //The Rock BTC

  {currency: 'EUR', issuer: 'rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q'}, //Snapswap EUR
  {currency: 'EUR', issuer: 'rhub8VRN55s94qWKDv6jmDy1pUykJzF3wq'}, //GateHub EUR
  {currency: 'EUR', issuer: 'rLEsXccBGNR3UPuPu2hUXPjziKC3qKSBun'}, //The Rock EUR

  {currency: 'CNY', issuer: 'rnuF96W4SZoCJmbHYBFoJZpR8eCaxNvekK'}, //RippleCN CNY
  {currency: 'CNY', issuer: 'razqQKzJRdB4UxFPWf5NEpEG3WMkmwgcXA'}, //RippleChina CNY
  {currency: 'CNY', issuer: 'rKiCet8SdvWxPXnAgYarFUXMh1zCPz432Y'}, //RippleFox CNY
  {currency: 'CNY', issuer: 'rM8199qFwspxiWNZRChZdZbGN5WrCepVP1'}, //DotPayco CNY

  {currency: 'JPY', issuer: 'r94s8px6kSw1uZ1MV98dhSRTvc6VMPoPcN'}, //TokyoJPY JPY
  {currency: 'JPY', issuer: 'rJRi8WW24gt9X85PHAxfWNPCizMMhqUQwg'}, //Ripple Market JPY
  {currency: 'JPY', issuer: 'r9ZFPSb1TFdnJwbTMYHvVwFK1bQPUCVNfJ'}, //Ripple Exchange Tokyo JPY
  {currency: 'JPY', issuer: 'rB3gZey7VWHYRqJHLoHDEJXJ2pEPNieKiS'}, //Mr Ripple JPY

  {currency: 'XAU', issuer: 'r9Dr5xwkeLegBeXq6ujinjSBLQzQ1zQGjH'}, //Ripple Singapore XAU
  {currency: 'XAU', issuer: 'rrh7rf1gV2pXAoqA8oYbpHd8TKv5ZQeo67'}, //GBI XAU
  {currency: 'STR', issuer: 'rKiCet8SdvWxPXnAgYarFUXMh1zCPz432Y'}, //Ripple Fox STR
  {currency: 'FMM', issuer: 'rKiCet8SdvWxPXnAgYarFUXMh1zCPz432Y'}, //Ripple Fox FMM
  {currency: 'MXN', issuer: 'rG6FZ31hDHN1K5Dkbma3PSB5uVCuVVRzfn'},  //Bitso MXN
  {currency: 'KRW', issuer: 'rPxU6acYni7FcXzPCMeaPSwKcuS2GTtNVN'}  //EXRP KRW
];

var conversionPairs = [];
currencies.forEach(function(currency) {

  if (currency.currency == 'XRP') {
    return;
  }

  conversionPairs.push({
    base    : {currency: 'XRP'},
    counter : currency
  });
});

if (!options.save) {
  hbaseOptions.logLevel = 1;
}

hbase = new Hbase(hbaseOptions);

// run aggregations
aggregateIssuedValue(options);

function aggregateIssuedValue(params) {

  var list = [];
  var start;
  var end;
  var live;

  if (!params) {
    params = {};
  }

  if (params.end && !params.start) {
    console.log('start date is required');
    process.exit(1);

  } else if (params.start && params.end) {
    start = smoment(params.start);
    end = smoment(params.end);

    if (!start || !end) {
      console.log('invalid start or end date');
      process.exit(1);
    }

  } else if (params.start) {
    start = smoment(params.start);
    end = smoment(params.start);

    if (!start) {
      console.log('invalid start date');
      process.exit(1);
    }
  }

  if (start && end) {
    start.moment.startOf('day');
    start.granularity = 'day';
    end.granularity = 'day';

    while (end.moment.diff(start.moment) >= 0) {
      list.push({
        time: smoment(start.format())
      });

      start.moment.add(1, 'day');
    }

  } else {
    list.push({
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

function handleAggregation(params, done) {

  getCurrencies()
  .then(getCapitalization)
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
            resolve(currencyList);
          }
        });

      } else {
        resolve(currencies);
      }
    });
  }

  function getCapitalization(currencyList) {

    return new Promise(function(resolve, reject) {

      // get capitalization data for each currency
      async.map(currencyList, function(c, asyncCallbackPair) {

        var options = {
          currency: c.currency,
          issuer: c.issuer,
          start: smoment(0),
          end: smoment(params.time),
          descending: true,
          adjusted: true,
          limit: 1
        };

        hbase.getCapitalization(options, function(err, resp) {

          if (err) {
            asyncCallbackPair(err);
            return;
          }

          asyncCallbackPair(null, {
            currency: c.currency,
            issuer: c.issuer,
            amount: resp.rows[0] ? resp.rows[0].amount : 0
          });
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

  function getRates(data) {

    return new Promise(function(resolve, reject) {

      // get exchanges for each pair
      async.map(data, function(d, asyncCallbackPair) {
        var start = smoment(params.time);
        var end = smoment(params.time);
        start.moment.subtract(14, 'days');

        var options = {
          base: { currency: 'XRP' },
          counter: { currency: d.currency, issuer: d.issuer },
          date: smoment(params.time)
        };

        hbase.getExchangeRate(options)
        .then(function(resp) {

          d.rate = resp || 0;

          asyncCallbackPair(null, d);
        }).catch(function(err) {
          asyncCallbackPair(err);
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

  function normalize(data) {
    var total = 0;
    data.forEach(function(d) {
      d.converted_amount = d.rate ? d.amount / d.rate : 0;
      total += d.converted_amount;
    });

    return {
      time: smoment(params.time).format(),
      total: total,
      exchange: { currency:'XRP' },
      exchangeRate: 1,
      components: data
    }
  }

  function save(result) {
    return new Promise(function(resolve, reject) {

      var table = 'agg_metrics';
      var rowkey = 'issued_value|';

      if (options.save) {
        rowkey += params.live ? 'live' : params.time.hbaseFormatStartRow();

        console.log('saving:', table, rowkey);
        return hbase.putRow(table, rowkey, result)
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
