'use strict';

var moment = require('moment');
var Promise = require('bluebird');
var BigNumber = require('bignumber.js');
var utils = require('../utils');
var Logger = require('../logger');

var LI_PAD = 12;
var I_PAD = 5;

/**
 * PaymentsAggregation
 */

function PaymentsAggregation(options) {
  var self = this;
  var logOpts = {
    scope: 'payments aggregation',
    file: options.logFile,
    level: options.logLevel
  };

  this.hbase = options.hbase;
  this.log = new Logger(logOpts);
  this.ready = true;
  this.currency = options.currency;
  this.issuer = options.issuer;
  this.pending = [ ];
  this.cached = {
    hour: { },
    day: { }
  };

  setImmediate(function() {
    self.aggregate();
  });

  // remove older data every hour
  this.purge = setInterval(function() {
    self.ready = false;

    var payments = moment.utc().startOf('hour').subtract(2, 'hour');
    var hour = moment.utc().startOf('day').subtract(1, 'day');
    var day = moment.utc().startOf('day').subtract(7, 'day');
    var time;

    // remove cached payments
    for (time in self.cached.hour) {
      if (payments.diff(time) > 0 &&
         self.cached.hour[time].payments) {

        self.cached.hour[time].payments = { };
      }
    }

    // remove cached payments
    for (time in self.cached.hour) {
      if (hour.diff(time) > 0) {
        delete self.cached.hour[time];
      }
    }

    // remove cached days
    for (time in self.cached.day) {
      if (day.diff(time) > 0) {
        delete self.cached.day[time];
      }
    }

    self.ready = true;

  }, 60 * 60 * 1000);
}

/**
 * aggregate
 * aggregate incoming payments
 */

PaymentsAggregation.prototype.aggregate = function() {
  var self = this;
  var incoming;
  var updated = {
    hour: { },
    day: { },
    week: { },
    month: { }
  };

  function aggregate() {
    self.aggregate();
  }

  if (!self.pending.length) {
    setTimeout(aggregate, 200);
    return;
  }

  if (!self.ready) {
    setTimeout(aggregate, 200);
    return;
  }

  incoming = self.pending;
  self.pending = [ ];
  self.ready = false;

  prepareHours()
  .then(aggregateHours)
  .then(prepareDays)
  .then(aggregateDays)
  .then(update)
  .nodeify(function(err, resp) {

    if (err) {
      self.log.error(err, resp);
    }

    // execute callback functions
    // for incoming exchanges
    incoming.forEach(function(i) {
      if (i.callback) {
        i.callback();
      }
    });

    self.ready = true;
    setImmediate(aggregate);
  });

  /**
   * fetchPayments
   */

  function fetchPayments(hour) {
    return new Promise(function(resolve, reject) {
      self.hbase.getPayments({
        currency: self.currency,
        issuer: self.issuer,
        start: moment.utc(hour),
        end: moment.utc(hour).add(1, 'hour'),
        descending: false
      }, function(err, resp) {
        if (err) {
          reject(err);
        } else {

          self.cached.hour[hour].payments = { };
          resp.rows.forEach(function(payment) {
            self.cached.hour[hour].payments[payment.rowkey] = payment;
          });
          resolve();
        }
      });
    });
  }

  /**
   * prepareHours
   */

  function prepareHours() {
    var hours = { };

    // determine hours to update
    incoming.forEach(function(row) {
      var hour = moment.unix(row.payment.time).utc().startOf('hour');
      if (!self.cached.hour[hour.format()]) {
        self.cached.hour[hour.format()] = { };
        hours[hour.format()] = true;
      }
    });

    // fetch all payments for
    // hours that are missing
    return Promise.map(Object.keys(hours), function(hour) {
      if (!self.cached.hour[hour].payments) {
        return fetchPayments(hour);
      }
    }).then(function() {

      // add the new payments
      incoming.forEach(function(row) {
        var hour = moment.unix(row.payment.time).utc().startOf('hour');
        var rowkey = utils.formatTime(row.payment.time) +
          '|' + utils.padNumber(row.payment.ledger_index, LI_PAD) +
          '|' + utils.padNumber(row.payment.tx_index, I_PAD);

        row.payment.rowkey = rowkey;
        self.cached.hour[hour.format()].payments[rowkey] = row.payment;
        self.cached.hour[hour.format()].updated = true;
      });
    });
  }

  /**
   * aggregateHours
   */

  function aggregateHours() {
    return Promise.map(Object.keys(self.cached.hour), function(hour) {
      var cached = self.cached.hour[hour];
      if (cached.updated) {
        cached.updated = false;
        cached.reduced = reduce(cached.payments);
        updated.hour[hour] = cached.reduced;
      }
    });
  }

  /**
   * fetchHour
   */

  function fetchHour(time) {
    return new Promise(function(resolve, reject) {
      var rowkey = 'hour|' + self.currency +
        '|' + (self.issuer || '') +
        '|' + utils.formatTime(time);

      self.hbase.getRow({
        table: 'agg_payments',
        rowkey: rowkey
      }, function(err, row) {
        if (err) {
          reject(err);
          return;
        }

        if (row) {
          row.count = Number(row.count);
          row.amount = Number(row.amount);
          row.average = Number(row.average);
        }

        if (!self.cached.hour[time]) {
          self.cached.hour[time] = { };
        }

        self.cached.hour[time].reduced = row;
        resolve();
      });
    });
  }

  /**
   * prepareDays
   */

  function prepareDays() {
    var hours = { };
    var start;
    var end;
    var now;

    // determine which hours we need
    for (var hour in updated.hour) {
      start = moment.utc(hour).startOf('day');
      end = moment.utc(start).add(1, 'day');
      now = moment.utc();

      if (!self.cached.day[start.format()]) {
        self.cached.day[start.format()] = { };
      }

      while (end.diff(start) > 0 &&
            now.diff(start) > 0) {
        if (!self.cached.hour[start.format()]) {
          hours[start.format()] = true;
        }

        start.add(1, 'hour');
      }
    }

    // fetch all hours that are missing
    return Promise.map(Object.keys(hours), fetchHour);
  }

  /**
   * aggregateDays
   */

  function aggregateDays() {
    var days = { };
    var hours = { };
    var hour;
    var day;
    var time;
    var end;

    // determine which days to update
    for (hour in updated.hour) {
      day = moment.utc(hour).startOf('day');
      days[day.format()] = day;
    }

    for (day in days) {
      time = days[day];
      end = moment.utc(time).add(1, 'day');

      while (end.diff(time) > 0) {
        hour = self.cached.hour[time.format()];
        hours[time.format()] = hour ? hour.reduced : undefined;
        time.add(1, 'hour');
      }

      self.cached.day[day].reduced = reduce(hours, true);
      updated.day[day] = self.cached.day[day].reduced;
    }
  }

  /**
   * update
   */

  function update() {
    var rows = { };
    var key = self.currency + '|' +
      (self.issuer || '');
    var rowkey;

    for (var interval in updated) {
      for (var time in updated[interval]) {
        rowkey = interval +
          '|' + key +
          '|' + utils.formatTime(time);

        rows[rowkey] = updated[interval][time];
        rows[rowkey].currency = self.currency;
        rows[rowkey].issuer = self.issuer;
        rows[rowkey].date = time;
      }
    }

    // console.log(rows);
    self.log.debug(Object.keys(rows));
    return self.hbase.putRows('agg_payments', rows);
  }

  /**
   * reduce
   */

  function reduce(rows, rereduce) {
    var amount = new BigNumber(0);
    var count = 0;

    for (var key in rows) {
      if (!rows[key]) {
        continue;

      } else if (rereduce) {
        amount = amount.plus(rows[key].amount);
        count += rows[key].count;

      } else {
        amount = amount.plus(rows[key].delivered_amount);
        count++;
      }
    }

    return {
      amount: amount.toString(),
      count: count,
      average: amount.dividedBy(count).toString()
    };
  }
};

/**
 * add
 */

PaymentsAggregation.prototype.add = function(payment, callback) {
  this.pending.push({
    payment: payment,
    callback: callback
  });
};

module.exports = PaymentsAggregation;
