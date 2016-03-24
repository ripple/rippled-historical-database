var config = require('../config/test.config.json');
var assert = require('assert');
var request = require('request');
var path = require('path');
var fs = require('fs');
var Server = require('../api/server');

var port = 7111;
var baseURL = 'http://localhost:' + port + '/v2/';
var server = new Server({
  postgres: undefined,
  hbase: config.hbase,
  port: port
});

var assetPath = path.resolve(__dirname + '/../api/gateways/gatewayAssets/');
var currencies = path.resolve(__dirname + '/../api/gateways/currencyAssets/');
var gatewayList = require('../api/gateways/gateways.json');
var bitstampLogo = fs.readFileSync(assetPath + '/bitstamp.logo.svg').toString();
var defaultCurrency = fs.readFileSync(currencies + '/default.svg').toString();
var logoUSD = fs.readFileSync(currencies + '/usd.svg').toString();

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
