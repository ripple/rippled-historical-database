var config = require('../config');
var request = require('request');
var assert = require('assert');
var moment = require('moment');
var utils = require('./utils');
var port = config.get('port') || 7111;

describe('account reports API endpoint', function() {

  it('should get reports by date range', function(done) {
    var account = 'r3fRiC42XCDHFkE4vLdJUhsVcx7hFbE5gU';
    var start = moment.utc('2015-01-14');
    var end = moment.utc('2015-01-16');
    var url = 'http://localhost:' + port + '/v2/accounts/' + account + '/reports';

    request({
      url: url,
      json: true,
      qs: {
        start: start.format(),
        end: end.format()
      }
    },
    function(err, res, body) {
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.reports.length, body.count);
      assert.strictEqual(body.reports.length, 3);
      body.reports.forEach(function(r) {
        assert.strictEqual(typeof r.date, 'string');
        assert.strictEqual(typeof r.high_value_received, 'string');
        assert.strictEqual(typeof r.high_value_sent, 'string');
        assert.strictEqual(typeof r.payments_received, 'number');
        assert.strictEqual(typeof r.payments_sent, 'number');
        assert.strictEqual(typeof r.receiving_counterparties, 'number');
        assert.strictEqual(typeof r.sending_counterparties, 'number');
        assert.strictEqual(typeof r.total_value, 'string');
        assert.strictEqual(typeof r.total_value_received, 'string');
        assert.strictEqual(typeof r.total_value_sent, 'string');
        assert(start.diff(moment.utc(r.date))<=0, 'date less than start time');
        assert(end.diff(moment.utc(r.date))>=0, 'date greater than end time');
      });
      done();
    });
  });

  it('should get reports in descending order', function(done) {
    var account = 'r3fRiC42XCDHFkE4vLdJUhsVcx7hFbE5gU';
    var start = moment.utc('2015-01-14');
    var end = moment.utc('2015-02-16');
    var url = 'http://localhost:' + port + '/v2/accounts/' + account + '/reports';
    var last = moment(end);

    request({
      url: url,
      json: true,
      qs: {
        start: start.format(),
        end: end.format(),
        descending: true
      }
    },
    function(err, res, body) {
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      body.reports.forEach(function(r) {
        assert(last.diff(r.date)>=0);
        last = moment.utc(r.date);
      });
      done();
    });
  });

  it('should get reports with counterparties', function(done) {
    var account = 'r3fRiC42XCDHFkE4vLdJUhsVcx7hFbE5gU';
    var start = moment.utc('2015-01-14');
    var end = moment.utc('2015-01-16');
    var url = 'http://localhost:' + port + '/v2/accounts/' + account + '/reports';

    request({
      url: url,
      json: true,
      qs: {
        accounts: true,
        start: start.format(),
        end: end.format()
      }
    },
    function(err, res, body) {
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      body.reports.forEach(function(a) {
        assert(Array.isArray(a.receiving_counterparties));
        assert(Array.isArray(a.sending_counterparties));
      });
      done();
    });
  });

  it('should get reports with individual payments', function(done) {
    var account = 'r3fRiC42XCDHFkE4vLdJUhsVcx7hFbE5gU';
    var start = moment.utc('2015-01-14');
    var end = moment.utc('2015-01-16');
    var url = 'http://localhost:' + port + '/v2/accounts/' + account + '/reports';

    request({
      url: url,
      json: true,
      qs: {
        payments: true,
        start: start.format(),
        end: end.format()
      }
    },
    function(err, res, body) {
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      body.reports.forEach(function(a) {
        assert(Array.isArray(a.payments));
        assert.strictEqual(a.payments.length, a.payments_received + a.payments_sent);
      });
      done();
    });
  });

  it('should get reports for a single date', function(done) {
    var date = '2015-01-14';
    var account = 'r3fRiC42XCDHFkE4vLdJUhsVcx7hFbE5gU';
    var url = 'http://localhost:' + port + '/v2/accounts/' + account + '/reports/' + date;

    request({
      url: url,
      json: true
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.reports.length, body.count);
      assert.strictEqual(body.reports.length, 1);
      done();
    });
  });

  it('should return an error if the date range is greater than 200', function(done) {
    var account = 'r3fRiC42XCDHFkE4vLdJUhsVcx7hFbE5gU';
    var start = moment.utc('2014-01-14');
    var end = moment.utc('2015-01-16');
    var url = 'http://localhost:' + port + '/v2/accounts/' + account + '/reports';

    request({
      url: url,
      json: true,
      qs: {
        start: start.format(),
        end: end.format()
      }
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'choose a date range less than 200 days');
      done();
    });
  });

  it('should return an error for an invalid date', function(done) {
    var date = '2015-01x';
    var account = 'r3fRiC42XCDHFkE4vLdJUhsVcx7hFbE5gU';
    var url = 'http://localhost:' + port + '/v2/accounts/' + account + '/reports/' + date;

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

  it('should return an error for an invalid start date', function(done) {
    var start = '2015x';
    var end = '2015-01-16';
    var account = 'r3fRiC42XCDHFkE4vLdJUhsVcx7hFbE5gU';
    var url = 'http://localhost:' + port + '/v2/accounts/' + account + '/reports';

    request({
      url: url,
      json: true,
      qs: {
        start: start,
        end: end
      }
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

  it('should return an error for an invalid end date', function(done) {
    var start = '2015-01-14';
    var end = '2015x';
    var account = 'r3fRiC42XCDHFkE4vLdJUhsVcx7hFbE5gU';
    var url = 'http://localhost:' + port + '/v2/accounts/' + account + '/reports';

    request({
      url: url,
      json: true,
      qs: {
        start: start,
        end: end
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
});
