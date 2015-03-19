var config      = require('./config');
var Promise     = require('bluebird');
var Storm       = require('./storm');
var Hbase       = require('./lib/hbase/hbase-client');
var Aggregation = require('./lib/aggregation/exchanges');
var BasicBolt   = Storm.BasicBolt;
var pairs       = { };
var bolt;

function ExchangesBolt() {
  var options = config.get('hbase2');

  if (!options) {
    options = config.get('hbase');
  }

  options.logLevel = config.get('logLevel');
  options.logFile  = config.get('logFile');

  this.hbase = new Hbase(options);

  BasicBolt.call(this);
}

ExchangesBolt.prototype = Object.create(BasicBolt.prototype);
ExchangesBolt.prototype.constructor = ExchangesBolt;

ExchangesBolt.prototype.process = function(tup, done) {
  var self = this;
  var ex   = tup.values[0];
  var pair = tup.values[1];

  if (!pairs[pair]) {
    self.log('new pair: ' + pair);
    pairs[pair] = new Aggregation({
      base     : ex.base,
      counter  : ex.counter,
      hbase    : self.hbase,
      logLevel : config.get('logLevel'),
      logFile  : config.get('logFile')
    });
    self.log('#pairs: ' + Object.keys(pairs).length);

  } else {
    self.log('new ex: ' + pair);
  }

  pairs[pair].add(ex, function(err, resp) {
    self.log(pair + ' aggregation finished');
  });

  //don't wait to ack
  done();
};


bolt = new ExchangesBolt();
bolt.run();
