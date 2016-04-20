var moment = require('moment');
var request = require('request');
var http = require('http');
var https = require('https');
var tf = 'YYYY-MM-DDTHH:mm:ss';

http.globalAgent.maxSockets = https.globalAgent.maxSockets = 2000;

var base = 'http://data-staging.ripple.com.global.prod.fastly.net/v2';
base = 'https://data-staging.ripple.com/v2';
//base = 'https://data.ripple.com/v2';
var pairs = [
  {
    base: 'USD+rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B',
    counter: 'XRP'
  }, {
    base: 'BTC+rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B',
    counter: 'XRP'
  }, {
    base: 'CNY+rKiCet8SdvWxPXnAgYarFUXMh1zCPz432Y',
    counter: 'XRP'
  }, {
    base: 'BTC+rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q',
    counter: 'XRP'
  }, {
    base: 'JPY+r94s8px6kSw1uZ1MV98dhSRTvc6VMPoPcN',
    counter: 'XRP'
  }, {
    base: 'USD+rhub8VRN55s94qWKDv6jmDy1pUykJzF3wq',
    counter: 'XRP'
  }
];

function landingPage() {
  var end = moment.utc().endOf('day');
  var start = moment.utc().startOf('minute');

  start.subtract(start.minutes() % 15, 'minutes')
    .subtract(1, 'day');

  var landing = [
    '/maintenance/ripplecharts',
    '/health/importer?verbose=true',
    '/accounts?reduce=true&start=2013-01-01',
    '/exchanges/XRP/USD+rhub8VRN55s94qWKDv6jmDy1pUykJzF3wq?limit=1000&interval=15minute&start=' + start.format(tf) + '&end=' + end.format(tf),
    '/exchanges/XRP/JPY+r94s8px6kSw1uZ1MV98dhSRTvc6VMPoPcN?limit=1000&interval=15minute&start=' + start.format(tf) + '&end=' + end.format(tf),
    '/exchanges/BTC+rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q/XRP?limit=1000&interval=15minute&start=' + start.format(tf) + '&end=' + end.format(tf),
    '/exchanges/XRP/CNY+rKiCet8SdvWxPXnAgYarFUXMh1zCPz432Y?limit=1000&interval=15minute&start=' + start.format(tf) + '&end=' + end.format(tf),
    '/exchanges/XRP/USD+rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B?limit=1000&interval=15minute&start=' + start.format(tf) + '&end=' + end.format(tf),
    '/exchanges/BTC+rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B/XRP?limit=1000&interval=15minute&start=' + start.format(tf) + '&end=' + end.format(tf),
    '/exchange_rates/USD+rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B/XRP',
    '/network/issued_value?&limit=1000',
    '/network/payment_volume?&limit=1000',
    '/network/exchange_volume?&limit=1000'
  ];

  return landing;
}

function marketsPage(base, counter) {
  var start = moment.utc().startOf('minute');
  start.subtract(start.minutes() % 5, 'minutes')
    .subtract(1, 'day');
  var end = moment(start).add(1, 'day')
    .add(5, 'minutes');

  var s2 = moment.utc().startOf('day');
  var e2 = moment.utc().endOf('day');
  var s3 = moment.utc().startOf('day').subtract(1, 'day');

  var markets = [
    '/maintenance/ripplecharts',
    '/health/importer?verbose=true',
    '/currencies/xrp.svg',
    '/currencies/USD.svg',
    '/currencies/BTC.svg',
    '/currencies/CNY.svg',
    '/currencies/JPY.svg',
    '/currencies/GBP.svg',
    '/currencies/NOK.svg',
    '/currencies/NZD.svg',
    '/currencies/MXN.svg',
    '/currencies/AUD.svg',
    '/currencies/STR.svg',
    '/currencies/FFM.svg',
    '/currencies/KRW.svg',
    '/currencies/XAU.svg',
    '/currencies/XAG.svg',
    '/currencies/SEK.svg',
    '/currencies/PEN.svg',
    '/currencies/LTC.svg',
    '/gateways/bitstamp/assets/logo.grayscale.svg',
    '/gateways/gatehub/assets/logo.grayscale.svg',
    '/gateways/ripplesingapore/assets/logo.grayscale.svg',
    '/gateways/snapswap/assets/logo.grayscale.svg',
    '/exchanges/' + base + '/' + counter +
      '?limit=1000&interval=5minute&start=' + start.format(tf) + '&end=' + end.format(tf),
    '/exchanges/' + base + '/' + counter +
      '?limit=1000&interval=1day&start=' + s2.format(tf) + '&end=' + e2.format(tf),
    '/exchanges/' + base + '/' + counter +
      '?limit=60&start=' + s3.format(tf) + '&end=' + e2.format(tf)
  ];

  return markets;
}

var stats = {
  requests: 0,
  success: 0,
  fail: 0,
  time: 0,
  start: Date.now()
};

var n = 1000;

function getPage(base, urls) {

  urls.forEach(function(url) {
    var d = Date.now();

    if (stats.requests === n) {
      return;
    }

    stats.requests++;

    request({
      url: base + url,
      json: true
    },
    function (err, res, body) {
      if (err) {
        stats.fail++;
        console.log(err);
        console.log()
      } else {
        stats.success++;
        d = (Date.now() - d)/1000;
        console.log(d, base + url);
        stats.time += d;
      }

      if (stats.success + stats.fail === n) {
        showStats();
      }
    });
  });
}

function getPages(n) {
  pairs.forEach(function(p, i) {
    setTimeout(function() {
      getPage(base, landingPage());
      getPage(base, marketsPage(p.base, p.counter));
    }, i*50);
  });
}

function showStats() {
  console.log('requests:', stats.requests);
  console.log('success:', stats.success);
  console.log('fail:', stats.fail);
  console.log('avg response time:', (stats.time/stats.success).toFixed(3));
  console.log('total time:', ((Date.now() - stats.start)/1000).toFixed(3));
  process.exit();
}

setInterval(getPages, 500);
getPages();





