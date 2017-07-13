var config = require('./config');
var Storm = require('./storm');
var Spout = Storm.Spout;
var stream = require('./lib/ledgerStream');
var nodemailer = require('nodemailer');
var transporter = nodemailer.createTransport();
var to = config.get('recipients');
var exec = require('child_process').exec;
var name = config.get('name') || 'unnamed';
var hdfs = require('./lib/hdfs')
var ledgerSpout;

var Logger = require('./lib/logger');
var log = new Logger({
  scope: 'ledger-stream-spout',
  file: config.get('logFile'),
  level: config.get('logLevel')
});

// handle uncaught exceptions
require('./exception')(log);

stream.live.api.on('error', handleAPIError);
stream.validator.importer.api.on('error', handleAPIError);

function handleAPIError(errorCode, errorMessage, data) {
  var kill = errorCode === 'badMessage' ? false : true;
  notify(errorCode + ': ' + errorMessage + ' data: ' + data, kill);
}

/**
 * notify
 */

function notify(message, kill) {
  var params = {
    from: 'Storm Import<storm-import@ripple.com>',
    to: to,
    subject: name + ' - rippleAPI error',
    html: 'The import topology received ' +
      'a rippleAPI error: <br /><br />\n' +
      '<blockquote><pre>' + message + '</pre></blockquote><br />\n'
  };

  if (kill) {
    params.html += 'Killing topology<br />\n';
  }

  transporter.sendMail(params, function() {
    if (kill) {
      killTopology();
    }
  });
}

/**
 * killTopology
 */

function killTopology() {
  exec('storm kill "ripple-ledger-importer" -w 0',
       function callback(e, stdout, stderr) {
    if (e) {
      log.error(e);
    }

    if (stderr) {
      log.error(stderr);
    }

    if (stdout) {
      log.info(stdout);
    }
  });
};

/**
 * LedgerStreamSpout
 */

function LedgerStreamSpout() {
  Spout.call(this);
  this.pending = { };
};

LedgerStreamSpout.prototype = Object.create(Spout.prototype);
LedgerStreamSpout.prototype.constructor = LedgerStreamSpout;

/**
 * nextTuple
 */

LedgerStreamSpout.prototype.nextTuple = function(done) {
  var self    = this;
  var timeout = 50;
  var tx;
  var id;

  //process all ledgers
  while (stream.ledgers.length) {
    stream.processNextLedger(function(err, row) {
      if (err) {
        self.log(err);
        return;
      }

      //emit fee summary
      self.emit({
        tuple: [row.feeSummary],
        id: row.ledger.ledger_index + '.fs',
        stream: 'feeSummaryStream'
      })

      //emit ledger header to HDFS
      self.emit({
        tuple: [row.ledger],
        id: row.ledger.ledger_index + '.lh',
        stream: 'HDFS_ledgerStream'
      })

      //if there are no transactions
      //just save the ledger
      if (!row.ledger.transactions.length) {

        stream.hbase.saveLedger(row.ledger, function(err, resp) {
          if (err) {
            self.log(err);
            self.log('unable to save ledger: ' + row.ledger.ledger_index);

          } else {
            self.log('ledger saved: ' + row.ledger.ledger_index);
          }

          // execute callback if it exists
          if (row.cb) {
            row.cb(err, resp);
          }
        });

      // already importing this ledger
      // this can happen because of the validator
      } else if (self.pending[row.ledger.ledger_index]) {
        self.log('Already importing: ' + row.ledger.ledger_index);
        if (row.cb) {
          row.cb('already importing this ledger');
        }

      // otherwise, wait till all transactions
      // are acked to save the ledger
      } else {
        self.pending[row.ledger.ledger_index] = {
          cb           : row.cb,
          ledger       : row.ledger,
          transactions : { },
          acks         : [ ]
        };

        self.log('# pending ledgers: ' + Object.keys(self.pending).length);
      }
    });
  }

  //emit one transaction per call
  if (stream.transactions.length) {
    tx = stream.transactions.shift();
    id = tx.ledger_index + '.' + tx.tx_index;

    //call the function immedately
    //if there are more transactions to process
    if (stream.transactions.length) timeout = 0;

    //the ledger may have
    //been removed if it failed
    if (self.pending[tx.ledger_index]) {

      //keep the transaction around
      //until its acked
      self.pending[tx.ledger_index].transactions[tx.tx_index] = {
        attempts : 1,
        tx       : tx
      };

      //emit transaction
      self.emit({
        tuple  : [tx],
        id     : id,
        stream : 'txStream'
      }, function(taskIds){
        self.log('tx: ' + id + ' sent to - ' + taskIds);
      });
    }
  }

  setTimeout(done, timeout);
};

/**
 * ack
 * report a transaction as success
 * and remove it from the pending list
 */

LedgerStreamSpout.prototype.ack = function(id, done) {
  var self  = this;
  var parts = id.split('.');
  var data  = self.pending[parts[0]];
  var total = 0


  //the ledger may already
  //have been removed because
  //of failed transactions
  if (!data) {
    self.log('no ledger: ' + id)
    done();
    return;
  }

  // HDFS ledger, fee summary, and transactions
  total = data.ledger.transactions.length + 2

  data.acks.push(parts[1]);

    self.log('Received ack for - ' + id +
             ' (' + data.acks.length + '/' + total + ')');

  //if we've got all the acks, save the ledger
  if (data.acks.length === total) {

    //increment ledger counter
    self.emit({
      tuple : [{
        time         : data.ledger.close_time,
        ledger_index : data.ledger.ledger_index,
        tx_count     : data.ledger.transactions.length
      }, 'ledger_count'],
      anchorTupleId : id,
      stream        : 'statsAggregation'
    });

    stream.hbase.saveLedger(data.ledger, function(err, resp) {
      if (err) {
        self.log(err);
        self.log('unable to save ledger: ' + data.ledger.ledger_index);

      } else {
        self.log('ledger saved: ' + data.ledger.ledger_index);
      }

      //execute callback, if it exists
      if (self.pending[parts[0]].cb) {
        self.pending[parts[0]].cb(err, resp);
      }

      //remove the pending ledger
      delete self.pending[parts[0]];
      done();
    });

  } else {
    done();
  }
};

/**
 * fail
 * report a transaction as failed
 * retry it up to 10 times
 */

LedgerStreamSpout.prototype.fail = function(id, done) {
  var self   = this;
  var parts  = id.split('.');
  var data   = this.pending[parts[0]];
  var txData = data && !isNaN(parts[1]) ? data.transactions[parts[1]] : null;
  var max = 2

  if (!data) {
    self.log('Received FAIL for - ' + id);

  } else if (txData && ++txData.attempts <= max) {
    self.log('Received FAIL for - ' + id + ' Retrying, attempt #' + txData.attempts);
    self.emit({
      tuple  : [txData.tx],
      id     : id,
      stream : 'txStream'
    }, function(taskIds) {
        self.log('tx: ' + id + ' resent to - ' + taskIds);
    });

  } else {
    if (txData) {
      self.log('Received FAIL for - ' + id + ' - Stopping after ' + max + ' attempts');
    } else {
      self.log('Received Fail for - ' + id);
    }

    //execute callback, if it exists
    if (data.cb) {
      data.cb('failed to save ledger');
    }

    //remove the failed ledger
    delete self.pending[parts[0]];
  }

  done();
};

ledgerSpout = new LedgerStreamSpout();
ledgerSpout.run();
stream.start();
