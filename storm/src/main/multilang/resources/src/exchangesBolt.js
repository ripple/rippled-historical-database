var config      = require('../config');
var Promise     = require('bluebird');
var Storm       = require('./lib/storm');
var Hbase       = require('./lib/hbase-client');
var Aggregation = require('./lib/exchangeAggregation');
var BasicBolt   = Storm.BasicBolt;
var pairs       = [ ];
var bolt;

function ExchangesBolt() {
  config.hbase.logLevel = config.logLevel;
  config.hbase.logFile  = config.logFile;
  
  this.hbase = new Hbase(config.hbase);

  //establish connection to hbase
  this.hbase.connect(); 
  
  BasicBolt.call(this);
}

ExchangesBolt.prototype = Object.create(BasicBolt.prototype);
ExchangesBolt.prototype.constructor = ExchangesBolt;

ExchangesBolt.prototype.process = function(tup, done) {
  var self = this;
  var ex   = tup.values[0];
  var pair = tup.values[1];
  var parsed;
  
  if (!pairs[pair]) {
    pairs[pair] = new Aggregation({
      base     : ex.base,
      counter  : ex.counter,
      hbase    : self.hbase,
      logLevel : config.logLevel,
      logFile  : config.logFile
    });
  } 
  
  pairs[pair].add(ex, function(err, resp) {
    self.log(err, resp);
    done();
  });  
};


bolt = new ExchangesBolt();
bolt.run();