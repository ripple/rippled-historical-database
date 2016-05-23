var config = require('./config');
var assert = require('assert');
var request = require('request');
var path = require('path');
var fs = require('fs');
var HBase = require('../lib/hbase/hbase-client');

var port = config.get('port') || 7111;
var prefix = config.get('prefix');
var hbaseConfig = config.get('hbase');

var hbase = new HBase(hbaseConfig);

var baseURL = 'http://localhost:' + port + '/v2/';
var assetPath = path.resolve(__dirname + '/../api/gateways/gatewayAssets/');
var currencies = path.resolve(__dirname + '/../api/gateways/currencyAssets/');
var gatewayList = require('./mock/gateways.json');
var bitstampLogo = fs.readFileSync(assetPath + '/bitstamp.logo.svg').toString();
var defaultCurrency = fs.readFileSync(currencies + '/default.svg').toString();
var logoUSD = fs.readFileSync(currencies + '/usd.svg').toString();

/**
 * normalize
 */

function normalize(name) {
  return name.toLowerCase().replace(/\W/g, '');
}

describe('setup mock data', function() {
  it('load data into hbase', function(done) {

    var table = 'gateways';
    var rows = {};

    gatewayList.forEach(function(d) {
      d.accounts.forEach(function(a) {
        var rowkey;
        for (var currency in a.currencies) {
          rowkey = currency + '|' + a.address;
          rows[rowkey] = {
            'f:address': a.address,
            'd:name': d.name,
            'f:normalized_name': normalize(d.name),
            'f:featured': a.featured ? '1' : '0',
            'f:type': 'issuer',
            'd:domain': d.domain
          };
        }
      });

      d.hotwallets.forEach(function(address) {
        var rowkey = 'AAA|' + address;
        rows[rowkey] = {
          'f:address': address,
          'd:name': d.name,
          'f:normalized_name': normalize(d.name),
          'f:featured': '0',
          'f:type': 'hot wallet',
          'd:domain': d.domain
        };
      });
    });

    hbase.putRows({
      table: table,
      rows: rows
    })
    .then(function(){
      done();
    })
    .catch(function(e) {
      assert.ifError(e);
    });
  });
});


describe('Gateways and Currencies APIs', function() {
  it('should get all gateways', function(done) {
    request({
      url: baseURL + 'gateways',
      json: true,
    },
      function (err, res, body) {
        assert.ifError(err);
        assert.strictEqual(res.statusCode, 200);
        assert.strictEqual(typeof body, 'object');
        for (var currency in body) {
          assert(Array.isArray(body[currency]));
        }
        done();
    });
  });

  it('should get a specific gateway by name', function(done) {
    request({
      url: baseURL + 'gateways/BTC 2 Ripple',
      json: true,
    },
      function (err, res, body) {
        assert.ifError(err);
        assert.strictEqual(res.statusCode, 200);
        assert.strictEqual(typeof body, 'object');
        assert.strictEqual(body.name, 'BTC 2 Ripple');
        done();
    });
  });

  it('should get a specific gateway by address', function(done) {
    request({
      url: baseURL + 'gateways/rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B',
      json: true,
    },
      function (err, res, body) {
        assert.ifError(err);
        assert.strictEqual(res.statusCode, 200);
        assert.strictEqual(typeof body, 'object');
        assert.strictEqual(body.name, 'Bitstamp');
        done();
    });
  });

  it('should get a specific gateway asset', function(done) {
    request({
      url: baseURL + 'gateways/bitstamp/assets/logo.svg'
    },
      function (err, res, body) {
        assert.ifError(err);
        assert.strictEqual(res.statusCode, 200);
        assert.strictEqual(bitstampLogo, body, 'logo not matched');
        done();
    });
  });

  it('should a specific currency logo', function(done) {
    request({
      url: baseURL + 'currencies/USD.svg'
    },
      function (err, res, body) {
        assert.ifError(err);
        assert.strictEqual(res.statusCode, 200);
        assert.strictEqual(logoUSD, body, 'logo not matched');
        done();
    });
  });

  it('should return a default logo for currencies not found', function(done) {
    request({
      url: baseURL + 'currencies/zzz.svg'
    },
      function (err, res, body) {
        assert.ifError(err);
        assert.strictEqual(res.statusCode, 200);
        assert.strictEqual(defaultCurrency, body, 'logo not matched');
        done();
    });
  });

  it('should return an error if a specific gateway is not found', function(done) {
    request({
      url: baseURL + 'gateways/zzz',
      json: true,
    },
      function (err, res, body) {
        assert.ifError(err);
        assert.strictEqual(res.statusCode, 404);
        assert.strictEqual(body.result, 'error');
        assert.strictEqual(body.message, 'gateway not found.');
        done();
    });
  });

  it('should return an error if a specific asset is not found', function(done) {
    request({
      url: baseURL + 'gateways/bitstamp/assets/zzz.svg',
      json: true,
    },
      function (err, res, body) {
        assert.ifError(err);
        assert.strictEqual(res.statusCode, 404);
        assert.strictEqual(body.result, 'error');
        assert.strictEqual(body.message, 'asset not found.');
        done();
    });
  });
});
