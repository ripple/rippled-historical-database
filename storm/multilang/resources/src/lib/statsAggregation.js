var Promise = require('bluebird');
var moment  = require('moment');
var Logger  = require('./modules/logger');
var Hbase   = require('./hbase-client');
var utils   = require('./utils');
var bolt;


function StatsAggregation(options) {
  var self = this;

  options.hbase.logLevel = options.logLevel;
  options.hbase.logFile  = options.logFile;

  var logOpts = {
    scope : 'stats-aggregation',
    file  : options.logFile,
    level : options.logLevel
  };

  this.log     = new Logger(logOpts);
  this.hbase   = new Hbase(options.hbase);
  this.ready   = false;
  this.pending = [ ];
  this.stats   = {
    hour    : { },
    day     : { },
    week    : { },
    ledgers : { },
  };

  this.load(function() {
    self.ready = true;
    self.aggregate();
  });

  //remove older data every hour
  this.purge = setInterval(function(){

    self.ready = false;

    var hour = moment.utc().startOf('hour').subtract(6, 'hours');
    var day  = moment.utc().startOf('day').subtract(1,  'days');
    var week = moment.utc().startOf('week').subtract(1, 'week');

    //remove cached hours
    for (time in self.stats.hour) {
      if (hour.diff(time) > 0) {
        delete self.stats.hour[time];
      }
    }

    //remove cached days
    for (time in self.stats.day) {
      if (day.diff(time) > 0) {
        delete self.stats.day[time];
      }
    }

    //remove cached weeks
    for (time in self.stats.week) {
      if (week.diff(time) > 0) {
        delete self.stats.week[time];
      }
    }

    self._ready = true;

  }, 60 * 60 * 1000);
}

StatsAggregation.prototype.load = function (callback) {
  var self = this;

  Promise.map(['hour','day','week'], function(interval) {
    return self.hbase.getStats({interval : interval});
  }).nodeify(function(err, resp) {
    if (err) {
      self.log.error(err);
      process.exit();
    }

    resp.forEach(function(row) {
      self.stats[row.interval][row.time] = row;
    });

    callback();
  });
};

StatsAggregation.prototype.aggregate = function () {
  var self    = this;
  var updated = { };
  var incomming;

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

  incoming.forEach(function(row) {
    var time = moment.unix(row.data.time).utc();
    var hour = moment(time).startOf('hour').format();
    var day  = moment(time).startOf('day').format();
    var week = moment(time).startOf('week').format();
    var prev, interval;

    if (row.label === 'transaction_type') {
      updateBucket('hour', hour, 'type', row.data.type);
      updateBucket('day',  day,  'type', row.data.type);
      updateBucket('week', week, 'type', row.data.type);

    } else if (row.label === 'transaction_result') {
      updateBucket('hour', hour, 'result', row.data.result);
      updateBucket('day',  day,  'result', row.data.result);
      updateBucket('week', week, 'result', row.data.result);

    } else if (row.label === 'transaction_count') {
      updateBucket('hour', hour, 'metric', 'transaction_count');
      updateBucket('day',  day,  'metric', 'transaction_count');
      updateBucket('week', week, 'metric', 'transaction_count');

    } else if (row.label === 'accounts_created') {
      updateBucket('hour', hour, 'metric', 'accounts_created', row.data.count);
      updateBucket('day',  day,  'metric', 'accounts_created', row.data.count);
      updateBucket('week', week, 'metric', 'accounts_created', row.data.count);

    } else if (row.label === 'ledger_count') {
      updateBucket('hour', hour, 'metric', 'ledger_count');
      updateBucket('day',  day,  'metric', 'ledger_count');
      updateBucket('week', week, 'metric', 'ledger_count');

      updateBucket('hour', hour, 'metric', 'tx_per_ledger');
      updateBucket('day',  day,  'metric', 'tx_per_ledger');
      updateBucket('week', week, 'metric', 'tx_per_ledger');

      updateBucket('hour', hour, 'metric', 'ledger_interval', time);
      updateBucket('day',  day,  'metric', 'ledger_interval', time);
      updateBucket('week', week, 'metric', 'ledger_interval', time);
      //save times for interval calc
      //self.stats.ledgers[row.data.ledger_index] = row.data.time;
      //prev = self.stats.ledgers[row.data.ledger_index - 1];
      //if (prev) {
      //  interval = row.data.time - prev;
      //}

      //console.log(interval);
    }

  });

  for(var key in updated) {
    var parts = key.split('|');
    var value = self.stats[parts[0]][parts[1]][parts[2]][parts[3]];

    self.updateStat({
      interval : parts[0],
      time     : parts[1],
      family   : parts[2],
      column   : parts[3],
      value    : value
    });
  }

  self.ready = true;
  setImmediate(aggregate);

  function aggregate () {
    self.aggregate();
  }

  function getBucket(interval, time) {
    if (!self.stats[interval][time]) {
      self.stats[interval][time] = {
        time     : time,
        interval : interval,
        type     : { },
        result   : { },
        metric   : {
          accounts_created  : 0,
          transaction_count : 0,
          ledger_count      : 0,
          tx_per_ledger     : 0.0,
          ledger_interval   : 0.0,
        }
      }
    }

    if (!self.stats[interval][time].increment) {
      self.stats[interval][time].increment = function (family, column, value) {
        if (typeof value === 'undefined') {
          value = 1;
        }

        if (!this[family][column]) {
          this[family][column]  = value;
        } else {
          this[family][column] += value;
        }
      }
    }

    return self.stats[interval][time];
  }

  function updateBucket(interval, time, family, column, value) {
    var bucket;
    var avg;
    var secs;

    bucket = getBucket(interval, time);

    if (column === 'tx_per_ledger') {
      avg = bucket.metric.transaction_count/bucket.metric.ledger_count;
      bucket.metric.tx_per_ledger = avg.toPrecision(5);

    } else if (column === 'ledger_interval') {
      if (bucket.first) {
        secs = value.diff(bucket.first, 'seconds');
        avg  = secs/bucket.metric.ledger_count;
        bucket.metric.ledger_interval = avg.toPrecision(5);

      } else {
        bucket.first = value;
        return;
      }

    } else {
      bucket.increment(family, column, value);
    }

    updated[interval + '|' + time + '|' + family + '|' + column] = true;
  }
};

StatsAggregation.prototype.update = function (stat) {
  this.pending.push(stat);
};

StatsAggregation.prototype.updateStat = function(options) {
  var self   = this;
  var rowkey = options.interval + '|' + utils.formatTime(options.time);
  var column = options.family   + ':' + options.column;
  var data   = { };

  data[column] = options.value;
  self.hbase.putRow('agg_stats', rowkey, data)
  .nodeify(function(err, resp) {
    if (err) {
      self.log.error(err);
    } else {
      self.log.debug('updated', rowkey, column, options.value);
    }
  });
};

module.exports = StatsAggregation;
