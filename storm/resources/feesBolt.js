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
  this.fees = new Aggregation();
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
