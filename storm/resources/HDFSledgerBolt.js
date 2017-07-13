var http = require('http')
var config = require('./config')
var Storm = require('./storm');
var hdfs = require('./lib/hdfs')
var BasicBolt = Storm.BasicBolt
var bolt;

http.globalAgent.maxSockets = 2000;

var Logger = require('./lib/logger');
var log = new Logger({
  scope: 'hdfs-transaction-bolt',
  file: config.get('logFile'),
  level: config.get('logLevel')
});

// handle uncaught exceptions
require('./exception')(log);


function LedgerBolt() {
  BasicBolt.call(this);
}

LedgerBolt.prototype = Object.create(BasicBolt.prototype);
LedgerBolt.prototype.constructor = LedgerBolt;

LedgerBolt.prototype.process = function(tup, done) {
  var self = this;
  var ledger = tup.values[0];
  var d = Date.now();

  hdfs.ingestLedgerHeader(ledger)
  .then(function() {
    d = (Date.now() - d) / 1000
    self.log('HDFS ledger saved: ' + ledger.ledger_index + ' in ' + d + 's');
    done()
  })
  .catch(done)
};

bolt = new LedgerBolt();
bolt.run();
