'use strict';

var config = require('../config');
var request = require('request');
var assert = require('assert');
var port = config.get('port') || 7111;

describe('exchanges rates API endpoint', function() {
  it('should get exchange rate', function(done) {
    var url = 'http://localhost:' + port + '/v2/exchange_rates/' +
      'XRP/USD+rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B';

    request({
      url: url,
      json: true,
      qs: {
        date: '2015-01-14'
      }
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.rate, '0.0000000');
      done();
    });
  });

  it('err on missing base issuer', function(done) {
    var url = 'http://localhost:' + port + '/v2/exchange_rates/' +
      'USD/XRP';

    request({
      url: url,
      json: true,
      qs: {
        date: '2015-01-14'
      }
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'base issuer is required');
      done();
    });
  });

  it('err on missing counter issuer', function(done) {
    var url = 'http://localhost:' + port + '/v2/exchange_rates/' +
      'XRP/USD';

    request({
      url: url,
      json: true,
      qs: {
        date: '2015-01-14'
      }
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'counter issuer is required');
      done();
    });
  });


  it('err on XRP with base issuer', function(done) {
    var url = 'http://localhost:' + port + '/v2/exchange_rates/' +
      'XRP+zzz/USD+zzz';

    request({
      url: url,
      json: true,
      qs: {
        date: '2015-01-14'
      }
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'XRP cannot have an issuer');
      done();
    });
  });

  it('err on XRP with counter issuer', function(done) {
    var url = 'http://localhost:' + port + '/v2/exchange_rates/' +
      'USD+zzz/XRP+zzz';

    request({
      url: url,
      json: true,
      qs: {
        date: '2015-01-14'
      }
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'XRP cannot have an issuer');
      done();
    });
  });


  it('err on future date', function(done) {
    var url = 'http://localhost:' + port + '/v2/exchange_rates/' +
      'XRP/USD+rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B';

    request({
      url: url,
      json: true,
      qs: {
        date: '9999-01-01'
      }
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'must not be a future date');
      done();
    });
  });
});

describe('normalization API endpoint', function() {
  it('normalize to XRP', function(done) {
    var url = 'http://localhost:' + port + '/v2/normalize';

    request({
      url: url,
      json: true,
      qs: {
        amount: 10,
        date: '2015-01-14',
        currency: 'USD',
        issuer: 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B'
      }
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.amount, '10');
      assert.strictEqual(body.converted, '0');
      assert.strictEqual(body.rate, '0.0000000');
      done();
    });
  });

  it('error on future date', function(done) {
    var url = 'http://localhost:' + port + '/v2/normalize';

    request({
      url: url,
      json: true,
      qs: {
        amount: 10,
        date: '9999-01-14',
        currency: 'USD',
        issuer: 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B'
      }
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'must not be a future date');
      done();
    });
  });

  it('error on invalid amount', function(done) {
    var url = 'http://localhost:' + port + '/v2/normalize';

    request({
      url: url,
      json: true,
      qs: {
        amount: 'zzzz',
        date: '2015-01-14',
        currency: 'USD',
        issuer: 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B'
      }
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'invalid amount');
      done();
    });
  });

  it('error on missing issuer', function(done) {
    var url = 'http://localhost:' + port + '/v2/normalize';

    request({
      url: url,
      json: true,
      qs: {
        amount: '10',
        date: '2015-01-14',
        currency: 'USD'
      }
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'issuer is required');
      done();
    });
  });

  it('error on XRP + issuer', function(done) {
    var url = 'http://localhost:' + port + '/v2/normalize';

    request({
      url: url,
      json: true,
      qs: {
        amount: '10',
        date: '2015-01-14',
        currency: 'XRP',
        issuer: 'ZZZ'
      }
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'XRP cannot have an issuer');
      done();
    });
  });

  it('error on missing exchange issuer', function(done) {
    var url = 'http://localhost:' + port + '/v2/normalize';

    request({
      url: url,
      json: true,
      qs: {
        amount: '10',
        date: '2015-01-14',
        exchange_currency: 'USD'
      }
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'issuer is required');
      done();
    });
  });

  it('error on XRP + exchange issuer', function(done) {
    var url = 'http://localhost:' + port + '/v2/normalize';

    request({
      url: url,
      json: true,
      qs: {
        amount: '10',
        date: '2015-01-14',
        exchange_currency: 'XRP',
        exchange_issuer: 'ZZZ'
      }
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'XRP cannot have an issuer');
      done();
    });
  });
});


