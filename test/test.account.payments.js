var config = require('../config');
var request = require('request');
var assert = require('assert');
var moment = require('moment');
var utils = require('./utils');
var port = config.get('port') || 7111;

describe('account payments API endpoint', function() {

  it('should make sure /accounts/:account/payments handles limit correctly', function(done) {
    var url = 'http://localhost:' + port + '/v2/accounts/rpjZUBy92h6worVCYERZcVCzgzgmHb17Dx/payments?limit=2';
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body.count, 2);
      assert.strictEqual(body.payments.length, 2);
      done();
    });
  });

  it('should make sure /accounts/:account/payments handles dates correctly', function(done) {
    var start= '2015-01-14T18:01:00';
    var end= '2015-01-14T18:40:45';
    var url = 'http://localhost:' + port + '/v2/accounts/rpjZUBy92h6worVCYERZcVCzgzgmHb17Dx/payments?'
                                         + 'start=' + start + '&end='+ end;
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.notStrictEqual(body.payments.length, 0);  // Make sure we test something
      body.payments.forEach( function(pay) {
        var d= moment.utc(pay.executed_time);
        assert.strictEqual( d.isBetween(moment.utc(start), moment.utc(end)) , true);
      });
      done();
    });
  });

  it('should make sure /accounts/:account/payments handles type correctly', function(done) {
    var url = 'http://localhost:' + port + '/v2/accounts/rpjZUBy92h6worVCYERZcVCzgzgmHb17Dx/payments?'
                                         + 'type=sent';
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.notStrictEqual(body.payments.length, 0);  // Make sure we test something
      body.payments.forEach( function(pay) {
        assert.strictEqual(pay.source, 'rpjZUBy92h6worVCYERZcVCzgzgmHb17Dx');
      });
      done();
    });
  });

  it('should make sure /accounts/:account/payments handles type correctly', function(done) {
    var url = 'http://localhost:' + port + '/v2/accounts/rGcSxmn1ibh5ZfCMAEu2iy7mnrb5nE6fbY/payments?'
                                         + 'type=received';
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.notStrictEqual(body.payments.length, 0);  // Make sure we test something
      body.payments.forEach( function(pay) {
        assert.strictEqual(pay.destination, 'rGcSxmn1ibh5ZfCMAEu2iy7mnrb5nE6fbY');
      });
      done();
    });
  });

  it('should filter by destination tag', function(done) {
    var account = 'rBeToNo4AwHaNbRX2n4BNCYKtpTyFLQwkj';
    var tag = 223051;
    var url = 'http://localhost:' + port +
      '/v2/accounts/' + account + '/payments' +
      '?destination_tag=' + tag;
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body.payments.length, 8);
      body.payments.forEach( function(pay) {
        assert.strictEqual(pay.destination, account);
        assert.strictEqual(pay.destination_tag, tag)
      });
      done();
    });
  });

  it('should filter by source tag', function(done) {
    var account = 'rUeXUxaMTH1pELvD2EkiHTRcM9FsH3v4d7';
    var tag = 1848687941;
    var url = 'http://localhost:' + port +
      '/v2/accounts/' + account + '/payments' +
      '?source_tag=' + tag;
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body.payments.length, 1);
      body.payments.forEach( function(pay) {
        assert.strictEqual(pay.source, account);
        assert.strictEqual(pay.source_tag, tag)
      });
      done();
    });
  });


  it('should make sure /accounts/:account/payments handles pagination correctly', function(done) {
    var url = 'http://localhost:' + port + '/v2/accounts/rpjZUBy92h6worVCYERZcVCzgzgmHb17Dx/payments?';
    utils.checkPagination(url, undefined, function(ref, i, body) {
      assert.strictEqual(body.payments.length, 1);
      assert.equal(body.payments[0].amount, ref.payments[i].amount);
      assert.equal(body.payments[0].tx_hash, ref.payments[i].tx_hash);
    }, done);
  });

  it('should make sure /accounts/:account/payments handles pagination correctly (the descending false version)', function(done) {
    var url = 'http://localhost:' + port + '/v2/accounts/rpjZUBy92h6worVCYERZcVCzgzgmHb17Dx/payments?';
    utils.checkPagination(url, undefined, function(ref, i, body) {
      assert.strictEqual(body.payments.length, 1);
      assert.equal(body.payments[0].amount, ref.payments[i].amount);
      assert.equal(body.payments[0].tx_hash, ref.payments[i].tx_hash);
    }, done);
  });

  it('should make sure /accounts/:account/payments handles empty response correctly', function(done) {
    var url = 'http://localhost:' + port + '/v2/accounts/rrrrUBy92h6worVCYERZcVCzgzgmHb17Dx/payments';
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body.payments.length, 0);
       assert.strictEqual(body.count, 0);
      done();
    });
  });

  it('should include a link header when marker is present', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/accounts/rpjZUBy92h6worVCYERZcVCzgzgmHb17Dx/payments?limit=1';
    var linkHeader = '<' + url +
      '&marker=rpjZUBy92h6worVCYERZcVCzgzgmHb17Dx|20150114185210|000011119940|00001>; rel="next"';

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
