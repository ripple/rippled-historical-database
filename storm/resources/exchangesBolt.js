var config = require('./config');
var Promise = require('bluebird');
var moment = require('moment');
var Storm = require('./storm');
var Aggregation = require('./lib/aggregation/exchanges');
var BasicBolt = Storm.BasicBolt;
var pairs = { };
var bolt;

var Logger = require('./lib/logger');
var log = new Logger({
  scope: 'exchanges-bolt',
  file: config.get('logFile'),
  level: config.get('logLevel')
});

// handle uncaught exceptions
require('./exception')(log);

function ExchangesBolt() {
  BasicBolt.call(this);
}

ExchangesBolt.prototype = Object.create(BasicBolt.prototype);
ExchangesBolt.prototype.constructor = ExchangesBolt;

ExchangesBolt.prototype.process = function(tup, done) {
  var self = this;
  var ex = tup.values[0];
  var pair = tup.values[1];

  if (!pairs[pair]) {
    self.log('new pair: ' + pair);
    pairs[pair] = new Aggregation({
      base: ex.base,
      counter: ex.counter,
      earliest: moment.unix(ex.time).utc()
    });
    self.log('#pairs: ' + Object.keys(pairs).length);

  } else {
    self.log('new ex: ' + pair);
  }

  pairs[pair].add(ex, function(err) {
    if (err) {
      self.log(err);
    } else {
      self.log(pair + ' aggregation finished');
    }
  });

  //don't wait to ack
  done();
};


bolt = new ExchangesBolt();
bolt.run();
