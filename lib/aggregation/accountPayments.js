var Promise = require('bluebird');
var moment  = require('moment');
var Logger  = require('../logger');
var Hbase   = require('../hbase/hbase-client');
var utils   = require('../utils');

/**
 * accountPaymentsAggregation
 */

function AccountPaymentsAggregation(options) {
  var self = this;

  options.hbase.logLevel = options.logLevel;
  options.hbase.logFile  = options.logFile;
  options.hbase.timeout  = 10000;

  var logOpts = {
    scope : 'account_payments-aggregation',
    file  : options.logFile,
    level : options.logLevel
  };

  this.log     = new Logger(logOpts);
  this.hbase   = new Hbase(options.hbase);
  this.ready   = true;
  this.pending = [ ];
  this.data    = { }

  self.aggregate();

  //remove older data every hour
  this.purge = setInterval(function(){

    self.ready = false;

    var day = moment.utc().startOf('day').subtract(12,  'hours');

    //remove cached days
    for (date in self.data) {
      if (day.diff(date) > 0) {
        delete self.data[date];
      }
    }

    self.ready = true;

  }, 60 * 60 * 1000);
}

/**
 * aggregate
 * aggregate incoming payments
 */

AccountPaymentsAggregation.prototype.aggregate = function () {
  var self       = this;
  var updated    = { };
  var bucketList = { };
  var incoming;

  if (!self.pending.length) {
    setTimeout(aggregate, 200);
    return;
  }

  if (!self.ready) {
    setTimeout(aggregate, 200);
    return;
  }

  incoming     = self.pending;
  self.pending = [ ];
  self.ready   = false;

  incoming.forEach(function(payment) {
    var date = moment.unix(payment.data.time).utc().startOf('day');

    if (!self.data[date.format()]) {
      self.data[date.format()] = { };
    }

    if (!self.data[date.format()][payment.data.source]) {
      bucketList[date.format() + '|' + payment.data.source] = {
        date    : date,
        account : payment.data.source
      };
    }

    if (!self.data[date.format()][payment.data.destination]) {
      bucketList[date.format() + '|' + payment.data.destination] = {
        date    : date,
        account : payment.data.destination
      };
    }
  });

  //get any from hbase that arent
  //already present
  Promise.map(Object.keys(bucketList), function(key) {
    return self.hbase.getAggregateAccountPayments(bucketList[key])
    .then(function(resp) {
      var date    = bucketList[key].date.format();
      var account = bucketList[key].account;
      self.data[date][account] = resp[0];
    });
  })
  .then(normalize) //normalize delivered amount to XRP
  .then(adjust)    //adjust buckets
  .then(update)    //save to hbase
  .nodeify(function(err) {

      if (err) {
        self.log.error(err);
      } else {
        self.log.debug('updated account payments', Object.keys(updated));
      }

      //ready for the next set
      self.ready = true;
      setImmediate(aggregate);
      return;
  });

  /**
   * aggregate
   * function to call from timeout
   */

  function aggregate () {
    self.aggregate();
  }

  /**
   * normalize
   * normalize the delivered amount
   * to XRP if possible
   */

  function normalize () {
    return Promise.map(incoming, function(payment) {
      return new Promise (function(resolve, reject) {

        var counter = { };
        var options = { };
        var change;

        if (payment.data.currency === 'XRP') {
          payment.normalized = Number(payment.data.delivered_amount);
          resolve();
          return;
        }

        //use the first issuer with the same
        //currency from destination balance changes
        for (var i=0; i<payment.data.destination_balance_changes.length; i++) {
          change = payment.data.destination_balance_changes[i];

          if (payment.data.currency === change.currency) {
            counter.issuer   = change.counterparty;
            counter.currency = change.currency;
            break;
          }
        }

        //this should not happen
        if (!counter.issuer) {
          payment.normalized = 0;
          resolve();
          return;
        }

        var options = {
          start      : moment.utc(0),
          end        : moment.unix(payment.data.time).utc(),
          base       : {currency:'XRP'},
          counter    : counter,
          descending : true,
          limit      : 50,
          reduce     : true
        };

        //use the last 50 trades prior to this
        //payment to determine an exchange rate
        self.hbase.getExchanges(options, function(err, data) {
          if (data) {
            payment.normalized = Number(payment.data.delivered_amount) / data.vwap;
          } else {
            payment.normalized = 0;
          }

          resolve();
        });
      });
    });
  }

  /**
   * adjust
   * adjust all buckets
   * with the incoming payments
   */

  function adjust () {
    incoming.forEach(function(payment) {

      var date   = moment.unix(payment.data.time).utc().startOf('day');
      var bucket = self.data[date.format()][payment.account];

      updated[date.format() + '|' + payment.account] = true;

      if (payment.account === payment.data.source) {
        bucket.payments_sent++;
        bucket.total_value_sent += payment.normalized;
        bucket.total_value      += payment.normalized;

        if (bucket.receiving_counterparties.indexOf(payment.data.destination) === -1) {
          bucket.receiving_counterparties.push(payment.data.destination);
        }

        if (payment.normalized > bucket.high_value_sent) {
          bucket.high_value_sent = payment.normalized;
        }

      } else {

        bucket = self.data[date.format()][payment.data.destination];

        bucket.payments_received++;
        bucket.total_value_received += payment.normalized;
        bucket.total_value          += payment.normalized;

        if (bucket.sending_counterparties.indexOf(payment.data.source) === -1) {
          bucket.sending_counterparties.push(payment.data.source);
        }

        if (payment.normalized > bucket.high_value_received) {
          bucket.high_value_received = payment.normalized;
        }
      }
    });
  }

  /**
   * update
   * save updated buckets
   */

  function update () {
    return Promise.map(Object.keys(updated), function(key) {
      var parts   = key.split('|');
      var date    = moment.utc(parts[0]);
      var account = parts[1];
      var rowkey  = utils.formatTime(date) + '|' + account;
      var data    = self.data[date.format()][account];
      return self.hbase.putRow('agg_account_payments', rowkey, data)
    });
  }
}

//add a payment to the queue
AccountPaymentsAggregation.prototype.add = function (payment) {
  this.pending.push(payment);
};

module.exports = AccountPaymentsAggregation;
