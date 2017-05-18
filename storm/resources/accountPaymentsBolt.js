var config      = require('./config');
var Promise     = require('bluebird');
var Storm       = require('./storm');
var Aggregation = require('./lib/aggregation/accountPayments');
var BasicBolt   = Storm.BasicBolt;
var bolt;

var Logger = require('./lib/logger');
var log = new Logger({
  scope: 'account-payments-bolt',
  file: config.get('logFile'),
  level: config.get('logLevel')
});

// handle uncaught exceptions
require('./exception')(log);

function AccountPaymentsBolt() {
  this.payments = new Aggregation();
  BasicBolt.call(this);
}

AccountPaymentsBolt.prototype = Object.create(BasicBolt.prototype);
AccountPaymentsBolt.prototype.constructor = AccountPaymentsBolt;

AccountPaymentsBolt.prototype.process = function(tup, done) {
  var self    = this;
  var payment = {
    data    : tup.values[0],
    account : tup.values[1]
  }

  //self.log(JSON.stringify(payment));
  self.payments.add(payment);

  //don't wait to ack
  done();
};

bolt = new AccountPaymentsBolt();
bolt.run();
