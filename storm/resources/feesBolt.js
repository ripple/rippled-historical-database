var config = require('./config');
var Storm = require('./storm');
var Aggregation = require('./lib/aggregation/fees');
var BasicBolt = Storm.BasicBolt;
var bolt;

var Logger = require('./lib/logger');
var log = new Logger({
  scope: 'fees-bolt',
  file: config.get('logFile'),
  level: config.get('logLevel')
});

// handle uncaught exceptions
require('./exception')(log);

function FeesBolt() {
  var options = config.get('hbase2');

  if (!options) {
    options = config.get('hbase');
  }

  options.logLevel = config.get('logLevel');
  options.logFile  = config.get('logFile');

  this.fees = new Aggregation(options);

  BasicBolt.call(this);
}

FeesBolt.prototype = Object.create(BasicBolt.prototype);
FeesBolt.prototype.constructor = FeesBolt;

FeesBolt.prototype.process = function(tup, done) {
  var self = this;
  var feeSummary = tup.values[0];

  self.fees.handleFeeSummary(feeSummary)
  .catch(function(e) {
    self.log(e);
  });

  //don't wait to ack
  done();
};

bolt = new FeesBolt();
bolt.run();
