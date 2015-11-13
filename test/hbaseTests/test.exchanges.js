var request = require('request');
var assert = require('assert');
var moment = require('moment');
var utils = require('../utils');
var config = require('../../config/import.config');
var port = config.get('port') || 7111;

describe('exchanges API endpoint', function() {
  it('should should get individual exchanges', function(done) {
    var account = 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B';
    var url = 'http://localhost:' + port + '/v2/exchanges/USD+rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q/xrp';
    var last = 0;

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.exchanges.length, body.count);
      assert.strictEqual(body.exchanges.length, 5);
      body.exchanges.forEach(function(ex) {
        assert.strictEqual(typeof ex.base_amount, 'string');
        assert.strictEqual(typeof ex.counter_amount, 'string');
        assert.strictEqual(typeof ex.rate, 'string');
        assert.strictEqual(typeof ex.offer_sequence, 'number');
        assert.strictEqual(typeof ex.ledger_index, 'number');
        assert.strictEqual(typeof ex.buyer, 'string');
        assert.strictEqual(typeof ex.seller, 'string');
        assert.strictEqual(typeof ex.taker, 'string');
        assert.strictEqual(typeof ex.provider, 'string');
        assert.strictEqual(typeof ex.executed_time, 'string');
        assert.strictEqual(typeof ex.tx_hash, 'string');
        assert.strictEqual(typeof ex.tx_type, 'string');

        assert(last <= ex.ledger_index);
        last = ex.ledger_index;
      });
      done();
    });
  });

  it('should should get individual exchanges by date', function(done) {
    var account = 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B';
    var url = 'http://localhost:' + port + '/v2/exchanges/USD+rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q/xrp';
    var start = moment.utc('2015-01-14T18:28:40');
    var end = moment.utc('2015-01-14T18:51:40');
    request({
      url: url,
      json: true,
      qs: {
        start: start.format(),
        end: end.format(),
      }
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.exchanges.length, body.count);
      assert.strictEqual(body.exchanges.length, 2);
      body.exchanges.forEach(function(ex) {
        assert(start.diff(moment.utc(ex.executed_time))<=0, 'executed time less than start time');
        assert(end.diff(moment.utc(ex.executed_time))>=0, 'executed time greater than end time');
      });
      done();
    });
  });

  it('should should get exchanges in descending order', function(done) {
    var account = 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B';
    var url = 'http://localhost:' + port + '/v2/exchanges/USD+rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q/xrp';
    var last = Infinity;

    request({
      url: url,
      json: true,
      qs: {
        descending: true
      }
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.exchanges.length, body.count);
      body.exchanges.forEach(function(ex) {
        assert(last >= ex.ledger_index);
        last = ex.ledger_index;
      });
      done();
    });
  });

  it('should make sure exchanges handles pagination correctly', function(done) {
    this.timeout(5000);
    var url = 'http://localhost:' + port + '/v2/exchanges/USD+rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q/xrp?';
    utils.checkPagination(url, undefined, function(ref, i, body) {
      assert.strictEqual(body.exchanges.length, 1);
      assert.equal(body.exchanges[0].base_amount, ref.exchanges[i].base_amount);
      assert.equal(body.exchanges[0].tx_hash, ref.exchanges[i].tx_hash);
    }, done);
  });

  it('should invert data when base and counter are inverted', function(done) {
    this.timeout(5000);
    var url1 = 'http://localhost:' + port + '/v2/exchanges/xrp/USD+rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q';
    var url2 = 'http://localhost:' + port + '/v2/exchanges/USD+rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q/xrp';
    var exchanges;
    var inverted;

    request({
      url: url1,
      json: true
    },
    function (err, res, body) {
      assert.ifError(err);
      exchanges = body.exchanges;

      request({
        url: url2,
        json: true
      },
      function (err, res, body) {
        assert.ifError(err);
        inverted = body.exchanges;
        exchanges.forEach(function(ex, i) {
          assert(ex.base_amount === inverted[i].counter_amount, 'base and counter not inverted');
          assert(ex.counter_amount === inverted[i].base_amount, 'counter and base not inverted');
        });
        done();
      });
    });
  });

  it('should should reduce exchanges to a single row', function(done) {
    var account = 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B';
    var url = 'http://localhost:' + port + '/v2/exchanges/USD+rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q/xrp';

    request({
      url: url,
      json: true,
      qs: {
        reduce : true,
      }
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.exchanges[0].count, 5);
      done();
    });
  });

  //there will not be any aggregates to be found
  it('should should get aggregate exchanges', function(done) {
    var account = 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B';
    var url = 'http://localhost:' + port + '/v2/exchanges/USD+rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q/xrp';

    request({
      url: url,
      json: true,
      qs: {
        interval : '1hour',
      }
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.exchanges.length, 1);
      assert.strictEqual(body.exchanges[0].count, 5);
      done();
    });
  });


  it('should return an error for an invalid start date', function(done) {
    var url = 'http://localhost:' + port + '/v2/exchanges/USD+rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q/xrp';

    request({
      url: url,
      json: true,
      qs: {
        start: '2015x',
      }
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'invalid start date format');
      done();
    });
  });

  it('should return an error for an invalid end time', function(done) {
    var url = 'http://localhost:' + port + '/v2/exchanges/USD+rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q/xrp';

    request({
      url: url,
      json: true,
      qs: {
        end: '2015x',
      }
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'invalid end date format');
      done();
    });
  });


  it('should return an error for an invalid interval', function(done) {
    var account = 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B';
    var url = 'http://localhost:' + port + '/v2/exchanges/USD+rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q/xrp';

    request({
      url: url,
      json: true,
      qs: {
        interval   : '3weeks',
      }
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'invalid interval: 3weeks');
      done();
    });
  });

  it('should return an error for an missing base issuer', function(done) {
    var account = 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B';
    var url = 'http://localhost:' + port + '/v2/exchanges/USD/xrp';

    request({
      url: url,
      json: true,
      qs: {
        interval   : '3weeks',
      }
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'base issuer is required');
      done();
    });
  });

  it('should return an error for an missing counter issuer', function(done) {
    var account = 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B';
    var url = 'http://localhost:' + port + '/v2/exchanges/xrp/USD';

    request({
      url: url,
      json: true,
      qs: {
        interval   : '3weeks',
      }
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'counter issuer is required');
      done();
    });
  });

  it('should return an error for issuer with XRP', function(done) {
    var account = 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B';
    var url = 'http://localhost:' + port + '/v2/exchanges/USD/xrp+rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q';

    request({
      url: url,
      json: true,
      qs: {
        interval   : '3weeks',
      }
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'XRP cannot have an issuer');
      done();
    });
  });

  it('should include a link header when marker is present', function(done) {
    var url = 'http://localhost:' + port + '/v2/exchanges/USD+rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q/xrp?limit=1';
    var linkHeader = '<' + url +
      '&marker=USD|rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q|XRP||20150114182720|000011119603|00012|00004>; rel="next"';

    request({
      url: url,
      json: true
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(res.headers.link, linkHeader);
      done();
    });
  });
});
