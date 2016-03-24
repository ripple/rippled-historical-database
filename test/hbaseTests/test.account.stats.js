var request = require('request');
var assert = require('assert');
var moment = require('moment');
var Promise = require('bluebird');
var utils = require('../utils');
var config = require('../../config/test.config');
var port = config.get('port') || 7111;
var txStats = require('../mock/account-stats-tx.json');
var valueStats = require('../mock/account-stats-value.json');
var prefix = config.get('prefix') || 'TEST_';
var HBase = require('../../lib/hbase/hbase-client');

var hbaseConfig = config.get('hbase');
var account = 'r3fRiC42XCDHFkE4vLdJUhsVcx7hFbE5gU';

hbaseConfig.prefix = prefix;
hbaseConfig.logLevel = 2;
hbaseConfig.max_sockets = 200;
hbaseConfig.timeout = 60000;

hbase = new HBase(hbaseConfig);

describe('account stats API endpoint', function() {

  before(function(done) {
    var rows = [];

    txStats.forEach(function(r) {
      var data = {
        'd:date': r.date,
        'd:transaction_count': r.transaction_count
      };
      var key;

      for (key in r.result) {
        data['result:' + key] = r.result[key];
      }

      for (key in r.type) {
        data['type:' + key] = r.type[key];
      }

      key = account + '|' + moment.utc(r.date).format('YYYYMMDDHHmmss');
      rows.push(hbase.putRow('agg_account_stats', key, data));
    });

    valueStats.forEach(function(r) {
      r.account = account;
      var key = account + '|' + moment.utc(r.date).format('YYYYMMDDHHmmss');
      rows.push(hbase.putRow('agg_account_balance_changes', key, r));
    });

    Promise.all(rows).nodeify(function(err, resp) {
      assert.ifError(err);
      done();
    });
  });

  it('should get transaction stats by date range', function(done) {
    var start = moment.utc('2015-01-01');
    var end = moment.utc('2015-01-16');
    var url = 'http://localhost:' + port + '/v2/accounts/' +
        account + '/stats/transactions';

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
      assert.strictEqual(body.rows.length, body.count);
      assert.strictEqual(body.rows.length, 15);
      body.rows.forEach(function(r) {
        assert.strictEqual(typeof r.type, 'object');
        assert.strictEqual(typeof r.result, 'object');
        assert(start.diff(r.date) <= 0);
        assert(end.diff(r.date) >= 0);
      });
      done();
    });
  });

  it('should get transaction stats in descending order', function(done) {
    var url = 'http://localhost:' + port + '/v2/accounts/' +
        account + '/stats/transactions?descending=true';
    var date;

    request({
      url: url,
      json: true,
    },
    function(err, res, body) {
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.rows.length, body.count);
      assert.strictEqual(body.rows.length, 20);
      body.rows.forEach(function(r) {
        if (date) {
          assert(date.diff(r.date) > 0);
        }

        date = moment(r.date);
      });
      done();
    });
  });

  it('should get value stats by date range', function(done) {
    var start = moment.utc('2015-01-01');
    var end = moment.utc('2015-01-16');
    var url = 'http://localhost:' + port + '/v2/accounts/' +
        account + '/stats/value';

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
      assert.strictEqual(body.rows.length, body.count);
      assert.strictEqual(body.rows.length, 15);
      body.rows.forEach(function(r) {
        assert.strictEqual(typeof r.account_value, 'string');
        assert.strictEqual(typeof r.balance_change_count, 'number');
        assert(start.diff(r.date) <= 0);
        assert(end.diff(r.date) >= 0);
      });
      done();
    });
  });

  it('should get value stats in descending order', function(done) {
    var url = 'http://localhost:' + port + '/v2/accounts/' +
        account + '/stats/value?descending=true';
    var date;

    request({
      url: url,
      json: true,
    },
    function(err, res, body) {
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.rows.length, body.count);
      assert.strictEqual(body.rows.length, 20);
      body.rows.forEach(function(r) {
        if (date) {
          assert(date.diff(r.date) > 0);
        }

        date = moment(r.date);
      });
      done();
    });
  });

  it('should include a link header when marker is present', function(done) {
    var url = 'http://localhost:' + port + '/v2/accounts/' +
        account + '/stats/transactions?limit=5';
    var linkHeader = '<' + url +
      '&marker=r3fRiC42XCDHFkE4vLdJUhsVcx7hFbE5gU|20150106000000>; rel="next"';

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

  it('should handle pagination correctly', function(done) {
    this.timeout(12000);
    var url = 'http://localhost:' + port + '/v2/accounts/' +
        account + '/stats/transactions?';

    utils.checkPagination(url, undefined, function(ref, i, body) {
      assert.strictEqual(body.rows.length, 1);
      assert.equal(body.rows[0].date, ref.rows[i].date);
      assert.equal(body.rows[0].transaction_count, ref.rows[i].transaction_count);
    }, done);
  });

  it('should error on invalid family', function(done) {
    var url = 'http://localhost:' + port + '/v2/accounts/' +
        account + '/stats/foo';

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'invalid family');
      done();
    });
  });

  it('should error on invalid start date', function(done) {
    var start = 'x2015-01-14T00:00';
    var end = '2015-01-14T00:00';
    var url = 'http://localhost:' + port + '/v2/accounts/' +
        account + '/stats/transactions' +
        '?start=' + start +
        '&end=' + end;

    request({
      url: url,
      json: true,
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

  it('should error on invalid end date', function(done) {
    var start = '2015-01-14T00:00';
    var end = 'x2015-01-14T00:00';
    var url = 'http://localhost:' + port + '/v2/accounts/' +
        account + '/stats/transactions' +
        '?start=' + start +
        '&end=' + end;

    request({
      url: url,
      json: true,
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
