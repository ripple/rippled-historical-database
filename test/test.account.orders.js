var config = require('../config');
var request = require('request');
var assert = require('assert');
var moment = require('moment');
var utils = require('./utils');
var port = config.get('port') || 7111;

describe('account offers API endpoint', function() {

  it('get account orders', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/accounts/rHsZHqa5oMQNL5hFm4kfLd47aEMYjPstpg/orders';
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(body.result, 'error')
      assert.strictEqual(body.message, 'rippled connection error.')
      done();
    });
  });
})
