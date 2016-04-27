var Promise = require('bluebird');
var smoment  = require('../smoment');
var Logger = require('../logger');
var Hbase = require('../hbase/hbase-client');
var utils = require('../utils');
var LI_PAD = 12;

/**
 * feesAggregation
 */

function FeesAggregation(options) {
  var self = this;

  var logOpts = {
    scope: 'fees-aggregation',
    file: options.logFile,
    level: options.logLevel
  };

  this._log = new Logger(logOpts);
  this._hbase = new Hbase(options);

  this._queue = [];
  this._cache = {};
  this._ready = true;

  // process new data
  setInterval(function() {
    self.aggregate();
  }, 1000);


  // purge older cached data
  setInterval(function() {
    var max = smoment();
    var date;

    max.moment.subtract(25, 'hours');
    for (var key in self._cache) {
      date = smoment(key.split('|')[1]);

      if (max.moment.diff(date.moment) > 0) {
        delete self._cache[key];
      }
    }
  }, 60 * 60 * 1000);
}

/**
 * handleFeeSummary
 */

FeesAggregation.prototype.handleFeeSummary = function(data) {
  var self = this;

  self._queue.push(data);

  return new Promise(function(resolve, reject) {
    data.callback = function(err, resp) {
      if (err) {
        reject(err);
      } else {
        resolve(resp);
      }
    }
  });
};

/**
 * aggregate
 */

FeesAggregation.prototype.aggregate = function() {
  var self = this;
  var rows = {};
  var list;
  var current;


  if (!this._ready || !this._queue.length) {
    return;
  }

  this._ready = false;

  var list = this._queue;
  var current = Promise.resolve();
  var rows = {};

  this._queue = [];


  Promise.all(list.map(function(d) {
    current = current.then(function() {
      return processFeeSummary(d);
    });

    return current;
  })).then(function() {
    return self._hbase.putRows({
      table: 'network_fees',
      rows: rows
    });

  }).nodeify(function(err) {
    self._ready = true;
    list.forEach(function(d) {
      d.callback(err);
    });
  });

  /**
   * processFeeSummary
   */

  function processFeeSummary(data) {
    addFeeSummary(data);
    return aggregateHour()
    .then(aggregateDay);

    function aggregateInterval(interval, timestamp) {
      return getInterval(interval, timestamp)
      .then(function(d) {
        merge(d, data);
        addFeeSummary(d);
      });
    }

    function aggregateHour() {
      var date = smoment(data.date);
      var timestamp;

      date.moment.startOf('hour');
      timestamp = date.hbaseFormatStartRow();
      return aggregateInterval('hour', timestamp);
    }

    function aggregateDay() {
      var date = smoment(data.date);
      var timestamp;

      date.moment.startOf('day');
      timestamp = date.hbaseFormatStartRow();
      return aggregateInterval('day', timestamp);
    }
  }

  /**
   * merge
   */

  function merge(data, incoming) {

    // initial data
    if (data.total === undefined) {
      data.total = incoming.total;
      data.tx_count = incoming.tx_count;
      data.avg = incoming.avg;
      data.min = incoming.min;
      data.max = incoming.max;

    // subsequent data
    } else {
      data.total += incoming.total;
      data.tx_count += incoming.tx_count;
      data.avg = data.total / data.tx_count;

      if (incoming.min && incoming.min < data.min) {
        data.min = incoming.min;
      }

      if (incoming.max > data.max) {
        data.max = incoming.max;
      }
    }

    return data;
  }

  /**
   * getInterval
   */

  function getInterval(interval, timestamp) {
    var rowkey = interval + '|' + timestamp;

    return new Promise(function(resolve, reject) {
      if (self._cache[rowkey]) {
        resolve(self._cache[rowkey]);
        return;
      }

      self._hbase.getRow({
        table: 'network_fees',
        rowkey: rowkey
      }, function(err, resp) {
        if (err) {
          reject(err);
          return;
        }

        if (resp) {
          resp.total = Number(resp.total);
          resp.tx_count = Number(resp.tx_count);
          resp.avg = Number(resp.avg);
          resp.min = Number(resp.min);
          resp.max = Number(resp.max);

          self._cache[rowkey] = resp;

        } else {
          self._cache[rowkey] = {
            interval: interval,
            date: smoment(timestamp).format()
          };
        }

        resolve(self._cache[rowkey]);
      });
    });
  }

  /**
   * addFeeSummary
   */

  function addFeeSummary(data) {
    var interval = data.interval || 'ledger';
    var rowkey;
    var date;

    var columns = {
      avg: round(data.avg, 6),
      max: data.max,
      min: data.min,
      total: round(data.total, 6),
      tx_count: data.tx_count,
      'f:date': data.date,
      'f:interval': interval,
      'f:ledger_index': data.ledger_index
    };

    if (interval === 'ledger') {
      rowkey = 'ledger|' + utils.padNumber(data.ledger_index, LI_PAD);
    } else {
      date = smoment(data.date);
      date.moment.startOf(interval);
      rowkey = interval + '|' + date.hbaseFormatStartRow();
    }

    rows[rowkey] = columns;
  }
};

/**
 * round
 */

function round(value, decimals) {
    return Number(Math.round(value + 'e' + decimals) + 'e-' + decimals);
}

module.exports = FeesAggregation;
