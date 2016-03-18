var Server = require('../../api/server');
var config = require('../../config/import.config');
var assert = require('assert');
var request = require('request');
var hbaseConfig = config.get('hbase');
var prefix = config.get('prefix') || 'TEST_';
var port = config.get('port') || 7111;
var server;

hbaseConfig.prefix = prefix;
hbaseConfig.topologyPrefix = prefix;
hbaseConfig.logLevel = 2;
hbaseConfig.max_sockets = 200;
hbaseConfig.timeout = 60000;

server = new Server({
  postgres: undefined,
  hbase: hbaseConfig,
  port: port
});

describe('server', function() {

  it('should handle duplicate query params', function(done) {
    var url = 'http://localhost:' + port + '/v2/accounts/rpjZUBy92h6worVCYERZcVCzgzgmHb17Dx/payments?type=sent&type=sent';
    request({
      url: url,
      json: true
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      done();
    });
  });
});
