var request = require('request');
var assert = require('assert');
var moment = require('moment');
var utils = require('../utils');
var config = require('../../config/import.config');
var port = config.get('port') || 7111;

describe('reports API endpoint', function() {

  it('should get reports', function(done) {
    var date = '2015-01-14T00:00:00Z';
    var url = 'http://localhost:' + port + '/v2/reports/' + date;

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.reports.length, body.count);
      assert.strictEqual(body.reports.length, 120);
      body.reports.forEach(function(a) {
        assert.strictEqual(a.date, date);
        assert.strictEqual(typeof a.account, 'string');
        assert.strictEqual(typeof a.high_value_received, 'string');
        assert.strictEqual(typeof a.high_value_sent, 'string');
        assert.strictEqual(typeof a.payments_received, 'number');
        assert.strictEqual(typeof a.payments_sent, 'number');
        assert.strictEqual(typeof a.receiving_counterparties, 'number');
        assert.strictEqual(typeof a.sending_counterparties, 'number');
        assert.strictEqual(typeof a.total_value, 'string');
        assert.strictEqual(typeof a.total_value_received, 'string');
        assert.strictEqual(typeof a.total_value_sent, 'string');
      });
      done();
    });
  });

  it('should get reports with counterparties', function(done) {
    var date = '2015-01-14T00:00:00Z';
    var url = 'http://localhost:' + port + '/v2/reports/' + date;

    request({
      url: url,
      json: true,
      qs: {
        accounts: true
      }
    },
    function(err, res, body) {
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.reports.length, body.count);
      assert.strictEqual(body.reports.length, 120);
      body.reports.forEach(function(a) {
        assert.strictEqual(a.date, date);
        assert(Array.isArray(a.receiving_counterparties));
        assert(Array.isArray(a.sending_counterparties));
      });
      done();
    });
  });

  it('should get reports with individual payments', function(done) {
    var date = '2015-01-14T00:00:00Z';
    var url = 'http://localhost:' + port + '/v2/reports/' + date;

    request({
      url: url,
      json: true,
      qs: {
        payments: true
      }
    },
    function(err, res, body) {
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.reports.length, body.count);
      assert.strictEqual(body.reports.length, 120);
      body.reports.forEach(function(a) {
        assert.strictEqual(a.date, date);
        assert(Array.isArray(a.payments));
        assert.strictEqual(a.payments.length, a.payments_received + a.payments_sent);
      });
      done();
    });
  });

  it('should handle pagination correctly', function(done) {
    this.timeout(7000);
    var date = '2015-02-09T00:00:00';
    var url = 'http://localhost:' + port + '/v2/reports/' + date + '?';

    utils.checkPagination(url, undefined, function(ref, i, body) {
      assert.strictEqual(body.reports.length, 1);
      assert.deepEqual(body.reports[0], ref.reports[i]);
    }, done);
  });

  it('should return an error for an invalid date', function(done) {
    var date = '2015-01x';
    var url = 'http://localhost:' + port + '/v2/reports/' + date;

    request({
      url: url,
      json: true
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'invalid date format');
      done();
    });
  });

  it('should include a link header when marker is present', function(done) {
    var date = '2015-01-14T00:00:00+00:00';
    var url = 'http://localhost:' + port + '/v2/reports/' + date + '?limit=10';
    var linkHeader = '<' + url +
      '&marker=20150114000000|r99ULJkNzWbHv34ARrfo8exKRD119YkoHE>; rel="next"';

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
