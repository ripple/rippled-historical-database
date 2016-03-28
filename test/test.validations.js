var config = require('./config');
var assert = require('assert');
var request = require('request');
var Promise = require('bluebird');
var smoment = require('../lib/smoment');
var Validations = require('../lib/validations/validations');
var mockValidations = require('./mock/validations.json');
var validations;

var hbaseConfig = config.get('hbase');
var port = config.get('port') || 7111;
var prefix = config.get('prefix') || 'TEST_';

hbaseConfig.prefix = prefix;
validations = new Validations(hbaseConfig);


describe('validations', function(done) {
  it('should save validations into hbase', function(done) {
    Promise.map(mockValidations, function(v) {
      return validations.handleValidation(v);
    }).then(function(resp) {
      assert.strictEqual(resp[0], '52E10A015D440A9D35EA0430D78437A2A2416FB3B73E6C56E869FBAF7EE10E47|n9LiNzfbTN5wEc9j2CM9ps7gQqAusVz8amg4gnsfHZ3DWHr2kkG1');
      assert.strictEqual(resp[1], 'EB26614C5E171C5A141734BAFFA63A080955811BB7AAE00D76D26FDBE9BC07A5|n9KDJnMxfjH5Ez8DeWzWoE9ath3PnsmkUy3GAHiVjE7tn7Q7KhQ2');
      assert.strictEqual(resp[2], undefined);
      assert.strictEqual(resp[5], undefined);
      done();

    }).catch(function(e) {
      assert.ifError(e);
    });
  });

  it('should save validator reports', function(done) {
    validations.updateReports()
    .then(function(resp) {
      done();

    }).catch(function(e) {
      assert.ifError(e);
    });
  });


  it('should get validator reports', function(done) {
    var date = smoment();
    var url = 'http://localhost:' + port +
        '/v2/network/validator_reports';

    date.moment.startOf('day');

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body.reports.length, 6);
      body.reports.forEach(function(r) {
        assert.strictEqual(r.date, date.format());
      });
      done();
    });
  });


  it('should get validator reports by date', function(done) {
    var date = smoment('2016-01-01');
    var url = 'http://localhost:' + port +
        '/v2/network/validator_reports?date='+date.format();

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body.reports.length, 0);
      done();
    });
  });

  it('should error on invalid date', function(done) {
    var date = 'zzz2015-01-14';
    var url = 'http://localhost:' + port +
        '/v2/network/validator_reports?date=' + date;

    request({
      url: url,
      json: true,
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

  it('should get get validator reports in CSV format', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/validator_reports?format=csv';

    request({
      url: url
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(res.headers['content-disposition'],
        'attachment; filename=validator reports.csv');
      done();
    });
  });
});
