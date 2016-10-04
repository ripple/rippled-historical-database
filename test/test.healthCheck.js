'use strict';

var config = require('./config');
var assert = require('assert');
var request = require('request');
var HBase = require('../lib/hbase/hbase-client');

var port = config.get('port') || 7111;
var baseURL = 'http://localhost:' + port + '/v2/health';
var prefix = config.get('prefix');

var hbaseConfig = config.get('hbase');
var hbase;

hbaseConfig.prefix = prefix;
hbaseConfig.max_sockets = 500;
hbaseConfig.timeout = 60000;
console.log(hbaseConfig);

hbase = new HBase(hbaseConfig);
describe('load mock data', function() {
  it('load control', function() {
    return hbase.putRow({
      table: 'control',
      rowkey: 'last_validated',
      columns: {
        close_time: '2014-10-04T00:07:30+00:00',
        ledger_index: 1234567
      }
    });
  });
});

describe('health check - API', function() {
  it('should check health', function(done) {
    request({
      url: baseURL + '/api'
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body, '0');
      done();
    });
  });

  it('should check health (verbose)', function(done) {
    request({
      url: baseURL + '/api?verbose=true',
      json: true
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body.score, 0);
      done();
    });
  });

  it('should use custom threshold', function(done) {
    request({
      url: baseURL + '/api?verbose=true&threshold=.0001',
      json: true
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body.score, 1);
      assert.strictEqual(body.message, 'response time exceeds threshold');
      done();
    });
  });

  it('should return an error for invalid threshold', function(done) {
    request({
      url: baseURL + '/api?threshold=z',
      json: true
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'invalid threshold');
      done();
    });
  });
});

describe('health check - Importer', function() {


  it('should check health', function(done) {
    request({
      url: baseURL + '/importer'
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body, '2');
      done();
    });
  });

  it('should check health (verbose)', function(done) {
    request({
      url: baseURL + '/importer?verbose=true',
      json: true
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body.score, 2);
      assert.strictEqual(body.last_validated_ledger, 1234567);
      assert.strictEqual(body.message, 'last ledger gap exceeds threshold');
      done();
    });
  });

  it('should use custom threshold', function(done) {
    request({
      url: baseURL + '/importer?verbose=true&threshold=Infinity',
      json: true
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body.score, 1);
      assert.strictEqual(body.ledger_gap_threshold, 'Infinity');
      assert.strictEqual(body.message, 'last validation gap exceeds threshold');
      done();
    });
  });
});

describe('health check - Nodes ETL', function() {
  it('should check health', function(done) {
    request({
      url: baseURL + '/nodes_etl'
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body, '1');
      done();
    });
  });

  it('should check health (verbose)', function(done) {
    request({
      url: baseURL + '/nodes_etl?verbose=true',
      json: true
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body.score, 1);
      assert.strictEqual(body.message, 'last imported data exceeds threshold');
      done();
    });
  });

  it('should use custom threshold', function(done) {
    request({
      url: baseURL + '/nodes_etl?verbose=true&threshold=Infinity',
      json: true
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body.score, 0);
      assert.strictEqual(body.gap_threshold, 'Infinity');
      done();
    });
  });
});

describe('health check - Validations ETL', function() {
  it('should check health', function(done) {
    request({
      url: baseURL + '/validations_etl'
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body, '1');
      done();
    });
  });

  it('should check health (verbose)', function(done) {
    request({
      url: baseURL + '/validations_etl?verbose=true',
      json: true
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body.score, 1);
      assert.strictEqual(body.message, 'last imported data exceeds threshold');
      done();
    });
  });

  it('should use custom threshold', function(done) {
    request({
      url: baseURL + '/validations_etl?verbose=true&threshold=Infinity',
      json: true
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body.score, 0);
      assert.strictEqual(body.gap_threshold, 'Infinity');
      done();
    });
  });
});
