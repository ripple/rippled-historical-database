var config      = require('./config');
var Promise     = require('bluebird');
var Storm       = require('./storm');
var Aggregation = require('./lib/aggregation/stats');
var BasicBolt   = Storm.BasicBolt;
var bolt;


function StatsBolt() {
  var options = config.get('hbase2');

  if (!options) {
    options = config.get('hbase');
  }

  options.logLevel = config.get('logLevel');
  options.logFile  = config.get('logFile');

  this.stats = new Aggregation(options);

  BasicBolt.call(this);
}

StatsBolt.prototype = Object.create(BasicBolt.prototype);
StatsBolt.prototype.constructor = StatsBolt;

StatsBolt.prototype.process = function(tup, done) {
  var self   = this;
  var stat   = {
    data  : tup.values[0],
    label : tup.values[1]
  }

  //self.log(JSON.stringify(stat));
  self.stats.update(stat);

  //don't wait to ack
  done();
};

bolt = new StatsBolt();
bolt.run();
