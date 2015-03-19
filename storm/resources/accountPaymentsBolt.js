var config      = require('./config');
var Promise     = require('bluebird');
var Storm       = require('./storm');
var Aggregation = require('./lib/aggregation/accountPayments');
var BasicBolt   = Storm.BasicBolt;
var bolt;


function AccountPaymentsBolt() {
  var options = config.get('hbase2');

  if (!options) {
    options = config.get('hbase');
  }

  options.logLevel = config.get('logLevel');
  options.logFile  = config.get('logFile');

  this.payments = new Aggregation(config);

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
