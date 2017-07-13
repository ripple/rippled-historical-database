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


function TransactionBolt() {
  BasicBolt.call(this);
}

TransactionBolt.prototype = Object.create(BasicBolt.prototype);
TransactionBolt.prototype.constructor = TransactionBolt;

TransactionBolt.prototype.process = function(tup, done) {
  var self = this;
  var tx = tup.values[0];
  var d = Date.now();

  hdfs.ingestTransaction(tx)
  .then(function() {
    d = (Date.now() - d) / 1000
    self.log('tx HDFS: ' + tx.ledger_index + '.' + tx.tx_index + ' saved in ' + d + 's')
    done()
  })
  .catch(done)
};

bolt = new TransactionBolt();
bolt.run();
