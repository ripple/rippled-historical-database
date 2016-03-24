var request = require('request');
var assert = require('assert');
var moment = require('moment');
var utils = require('../utils');
var config = require('../../config/test.config');
var port = config.get('port') || 7111;

describe('accounts API endpoint', function() {

  it('should get individual accounts created', function(done) {
    var url = 'http://localhost:' + port + '/v2/accounts';
    var last = 0;

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.accounts.length, body.count);
      assert.strictEqual(body.accounts.length, 13);
      body.accounts.forEach(function(a) {
        assert.strictEqual(typeof a.ledger_index, 'number');
        assert.strictEqual(typeof a.initial_balance, 'string');
        assert.strictEqual(typeof a.inception, 'string');
        assert.strictEqual(typeof a.account, 'string');
        assert.strictEqual(typeof a.parent, 'string');
        assert.strictEqual(typeof a.tx_hash, 'string');
        assert(last <= a.ledger_index);
        last = a.ledger_index;
      });
      done();
    });
  });

  it('should get individual accounts created by date', function(done) {
    var url = 'http://localhost:' + port + '/v2/accounts';
    var start = moment.utc('2013-01-14T18:28:40');
    var end = moment.utc('2013-07-01');

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
      assert.strictEqual(body.accounts.length, body.count);
      assert.strictEqual(body.accounts.length, 9);
      body.accounts.forEach(function(a) {
        assert(start.diff(moment.utc(a.inception))<=0, 'inception less than start time');
        assert(end.diff(moment.utc(a.inception))>=0, 'inception greater than end time');
      });
      done();
    });
  });

  it('get individual accounts created in descending order', function(done) {
    var url = 'http://localhost:' + port + '/v2/accounts';
    var last = Infinity;

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
      assert.strictEqual(body.accounts.length, body.count);
      assert.strictEqual(body.accounts.length, 13);
      body.accounts.forEach(function(a) {
        assert(last >= a.ledger_index);
        last = a.ledger_index;
      });
      done();
    });
  });

  it('should get individual accounts created by parent', function(done) {
    var url = 'http://localhost:' + port + '/v2/accounts';
    var parent = 'rMTzGg7nPPEMJthjgEBfiPZGoAM7MEVa1r';

    request({
      url: url,
      json: true,
      qs: {
        parent: parent
      }
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.accounts.length, body.count);
      assert.strictEqual(body.accounts.length, 3);
      body.accounts.forEach(function(a) {
        assert.strictEqual(a.parent, parent);
      });
      done();
    });
  });

  it('should make sure accounts handles pagination correctly', function(done) {
    this.timeout(5000);
    var url = 'http://localhost:' + port + '/v2/accounts?';
    utils.checkPagination(url, undefined, function(ref, i, body) {
      assert.strictEqual(body.accounts.length, 1);
      assert.equal(body.accounts[0].base_amount, ref.accounts[i].base_amount);
      assert.equal(body.accounts[0].tx_hash, ref.accounts[i].tx_hash);
    }, done);
  });

  it('get aggregated accounts created', function(done) {
    var url = 'http://localhost:' + port + '/v2/accounts';

    request({
      url: url,
      json: true,
      qs: {
        interval: 'hour'
      }
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.accounts.length, body.count);
      assert.strictEqual(body.accounts.length, 3);
      assert.strictEqual(body.accounts[0].date, '2013-06-11T20:00:00Z');
      assert.strictEqual(body.accounts[1].date, '2013-10-25T10:00:00Z');
      assert.strictEqual(body.accounts[0].count, 9);
      assert.strictEqual(body.accounts[1].count, 3);
      done();
    });
  });

  it('should get reduced accounts created', function(done) {
    var url = 'http://localhost:' + port + '/v2/accounts';
    var start = moment.utc('2013-06-11');
    var end = moment.utc('2013-07-01');

    request({
      url: url,
      json: true,
      qs: {
        reduce: true,
        start: start.format(),
        end: end.format()
      }
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.count, 9);
      done();
    });
  });

  it('should get reduced accounts created by parent', function(done) {
    var url = 'http://localhost:' + port + '/v2/accounts';
    var parent = 'rMTzGg7nPPEMJthjgEBfiPZGoAM7MEVa1r';

    request({
      url: url,
      json: true,
      qs: {
        reduce: true,
        parent: parent
      }
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.count, 3);
      done();
    });
  });

  it('should get and individual account', function(done) {
    var account = 'rGTvj2qFEYunK7vnWeWmxwvTcL2svvVP7b';
    var url = 'http://localhost:' + port + '/v2/accounts/' + account;

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(typeof body.account_data, 'object');
      assert.strictEqual(body.account_data.account, account);
      assert.strictEqual(typeof body.account_data.parent, 'string');
      assert.strictEqual(typeof body.account_data.initial_balance, 'string');
      assert.strictEqual(typeof body.account_data.inception, 'string');
      assert.strictEqual(typeof body.account_data.tx_hash, 'string');
      assert.strictEqual(typeof body.account_data.ledger_index, 'number');
      done();
    });
  });

  it('should return an error if an account is not found', function(done) {
    var account = 'zzz';
    var url = 'http://localhost:' + port + '/v2/accounts/' + account;

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 404);
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'Account not found');
      done();
    });
  });

  it('should return an error for an invalid start date', function(done) {
    var url = 'http://localhost:' + port + '/v2/accounts';

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
    var url = 'http://localhost:' + port + '/v2/accounts';

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
    var url = 'http://localhost:' + port + '/v2/accounts';

    request({
      url: url,
      json: true,
      qs: {
        interval: 'months',
      }
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'invalid interval');
      done();
    });
  });

  it('should return an error for interval and reduce', function(done) {
    var url = 'http://localhost:' + port + '/v2/accounts';

    request({
      url: url,
      json: true,
      qs: {
        interval: 'week',
        reduce: true
      }
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'cannot use reduce with interval');
      done();
    });
  });

  it('should include a link header when marker is present', function(done) {
    var url  = 'http://localhost:' + port + '/v2/accounts?limit=1';
    var linkHeader = '<' + url +
      '&marker=20130611200120|000001021029|00001>; rel="next"';

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
