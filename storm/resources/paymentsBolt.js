'use strict';

var config = require('./config');
var Storm = require('./storm');
var Hbase = require('./lib/hbase/hbase-client');
var Aggregation = require('./lib/aggregation/payments');
var BasicBolt = Storm.BasicBolt;
var currencies = { };
var bolt;

function PaymentsBolt() {
  var options = config.get('hbase2');

  if (!options) {
    options = config.get('hbase');
  }

  options.logLevel = config.get('logLevel');
  options.logFile = config.get('logFile');

  this.hbase = new Hbase(options);

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
      issuer: p.issuer,
      hbase: self.hbase,
      logLevel: config.get('logLevel'),
      logFile: config.get('logFile')
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
