'use strict';

var config = require('./config');
var Storm = require('./storm');
var Aggregation = require('./lib/aggregation/payments');
var BasicBolt = Storm.BasicBolt;
var currencies = { };
var bolt;

var Logger = require('./lib/logger');
var log = new Logger({
  scope: 'payments-bolt',
  file: config.get('logFile'),
  level: config.get('logLevel')
});

// handle uncaught exceptions
require('./exception')(log);

function PaymentsBolt() {
  BasicBolt.call(this);
}

PaymentsBolt.prototype = Object.create(BasicBolt.prototype);
PaymentsBolt.prototype.constructor = PaymentsBolt;

PaymentsBolt.prototype.process = function(tup, done) {
  var self = this;
  var p = tup.values[0];
  var key = tup.values[1];

  if (!currencies[key]) {
    currencies[key] = new Aggregation({
      currency: p.currency,
      issuer: p.issuer
    });

    self.log('#currencies: ' + Object.keys(currencies).length);

  } else {
    self.log('new payment: ' + key);
  }

  currencies[key].add(p, function(err) {
    if (err) {
      self.log(err);
    } else {
      self.log(key + ' aggregation finished');
    }
  });

  // don't wait to ack
  done();
};


bolt = new PaymentsBolt();
bolt.run();
