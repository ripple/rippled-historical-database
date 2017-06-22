var config = require('../config');
var request = require('request');
var assert = require('assert');
var moment = require('moment');
var utils = require('./utils');
var port = config.get('port') || 7111;

describe('account balance API endpoint', function() {

  it('get account balances', function(done) {
    var url = 'http://localhost:' + port + '/v2/accounts/rHsZHqa5oMQNL5hFm4kfLd47aEMYjPstpg/balances';
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(body.result, 'error')
      assert.strictEqual(body.message, 'unable to retrieve balances')
      done();
    });
  });
})
