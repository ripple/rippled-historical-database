var request = require('request');
var assert = require('assert');
var moment = require('moment');
var utils = require('../utils');
var config = require('../../config/import.config');
var port = config.get('port') || 7111;

describe('stats API endpoint', function() {

  it('should get stats', function(done) {
    var url = 'http://localhost:' + port + '/v2/stats';
    var date = moment.utc('2013-01-01');

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.stats.length, body.count);
      assert.strictEqual(body.stats.length, 4);
      body.stats.forEach(function(s) {
        assert.strictEqual(typeof s.date, 'string');
        assert.strictEqual(typeof s.type, 'object');
        assert.strictEqual(typeof s.result, 'object');
        assert.strictEqual(typeof s.metric, 'object');
        assert(date.diff(s.date)<=0, 'date not greater than previous date');
        date = moment.utc(s.date);
      });
      done();
    });
  });

  it('should get stats by time', function(done) {
    var url = 'http://localhost:' + port + '/v2/stats';
    var start = moment.utc('2013-01-01');
    var end = moment.utc('2014-01-01');

    request({
      url: url,
      json: true,
      qs: {
        start: start.format(),
        end: end.format(),
      }
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.stats.length, body.count);
      assert.strictEqual(body.stats.length, 2);
      body.stats.forEach(function(s) {
        assert(start.diff(moment.utc(s.date))<=0, 'executed time less than start time');
        assert(end.diff(moment.utc(s.date))>=0, 'executed time greater than end time');
      });
      done();
    });
  });

  it('should get stats by time in descending order', function(done) {
    var url = 'http://localhost:' + port + '/v2/stats';
    var date = moment.utc('9999-12-31');

    request({
      url: url,
      json: true,
      qs: {
        descending: true
      }
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.stats.length, body.count);
      assert.strictEqual(body.stats.length, 4);
      body.stats.forEach(function(s) {
        assert(date.diff(s.date)>=0, 'date not less than previous date');
        date = moment.utc(s.date);
      });
      done();
    });
  });

  it('should make sure stats handles pagination correctly', function(done) {
    this.timeout(5000);
    var url = 'http://localhost:' + port + '/v2/stats?';
    utils.checkPagination(url, undefined, function(ref, i, body) {
      assert.strictEqual(body.stats.length, 1);
      assert.equal(body.stats[0].date, ref.stats[i].date);
      assert.equal(body.stats[0].type.Payment, ref.stats[i].type.Payment);
    }, done);
  });

  it('should get stats by family', function(done) {
    var family = 'metric';
    var url = 'http://localhost:' + port + '/v2/stats/' + family;

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      body.stats.forEach(function(s) {
        assert.strictEqual(typeof s.date, 'string');
        assert.strictEqual(typeof s.ledger_count, 'number');
        assert.strictEqual(typeof s.tx_per_ledger, 'number');
      });
      done();
    });
  });

  it('should get stats by family and metric', function(done) {
    var family = 'metric';
    var metric = 'ledger_count';
    var url = 'http://localhost:' + port + '/v2/stats/' + family + '/' + metric;

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      body.stats.forEach(function(s) {
        assert.strictEqual(typeof s.date, 'string');
        assert.strictEqual(typeof s.ledger_count, 'number');
      });
      done();
    });
  });

  it('should give an error for invalid family', function(done) {
    var family = 'metricz';
    var metric = 'ledger_count';
    var url = 'http://localhost:' + port + '/v2/stats/' + family + '/' + metric;

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'invalid family, use: type, result, metric');
      done();
    });
  });

  it('should give an empty result for a non-existent stat', function(done) {
    var family = 'metric';
    var metric = 'ledger_countz';
    var url = 'http://localhost:' + port + '/v2/stats/' + family + '/' + metric;

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.stats.length, 0);
      done();
    });
  });

  it('should return an error for an invalid start date', function(done) {
    var url = 'http://localhost:' + port + '/v2/stats';

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

  it('should return an error for an invalid end date', function(done) {
    var url = 'http://localhost:' + port + '/v2/stats';

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

  it('should include a link header when marker is present', function(done) {
    var url = 'http://localhost:' + port + '/v2/stats?limit=1';
    var linkHeader = '<' + url +
      '&marker=day|20131025000000>; rel="next"';

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
