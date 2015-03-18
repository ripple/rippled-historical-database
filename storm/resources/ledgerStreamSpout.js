var config       = require('./config');
var Storm        = require('./storm');
var LedgerStream = require('./lib/ledgerStream');
var Spout        = Storm.Spout;
var ledgerSpout;
var stream;
var options = {
  logLevel : config.get('logLevel'),
  logFile  : config.get('logFile'),
  ripple   : config.get('ripple'),
  hbase    : config.get('hbase')
}

stream = new LedgerStream(options);

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
  var timeout = 100;
  var tx;
  var id;

  //process all ledgers
  while (stream.ledgers.length) {
    stream.processNextLedger(function(err, row) {
      if (err) {
        self.log(err);
        return;
      }

      //if there are no transactions
      //just save the ledger
      if (!row.ledger.transactions.length) {
        stream.hbase.saveLedger(row.ledger, function(err, resp) {
          if (err) {
            self.log.error(err);
            self.log('unable to save ledger: ' + row.ledger.ledger_index);

          } else {
            self.log('ledger saved: ' + row.ledger.ledger_index);
          }

          //execute callback if it exists
          if (row.cb) {
            row.cb(err, resp);
          }
        });

      //otherwise, wait till all transactions
      //are acked to save the ledger
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
    id = tx.ledger_index + '|' + tx.tx_index;

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
  var parts = id.split('|');
  var data  = self.pending[parts[0]];

  self.log('Received ack for - ' + id);

  //the ledger may already
  //have been removed because
  //of failed transactions
  if (!data) {
    done();
    return;
  }

  data.acks.push(parts[1]);

  //if we've acked all transactions, save the ledger
  if (data.acks.length == data.ledger.transactions.length) {

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
  var parts  = id.split('|');
  var data   = this.pending[parts[0]];
  var txData = data ? data.transactions[parts[1]] : null;

  if (!data) {
    self.log('Received FAIL for - ' + id + ' Stopping, ledger failed');

  } else if (++txData.attempts <= 3) {
    self.log('Received FAIL for - ' + id + ' Retrying, attempt #' + txData.attempts);
    self.emit({
      tuple  : [txData.tx],
      id     : id,
      stream : 'txStream'
    }, function(taskIds) {
        self.log('tx: ' + id + ' resent to - ' + taskIds);
    });

  } else {
    self.log('Received FAIL for - ' + id + ' - Stopping after 3 attempts');

    //remove the failed ledger
    delete self.pending[parts[0]];
  }

  done();
};

ledgerSpout = new LedgerStreamSpout();
ledgerSpout.run();
stream.start();
