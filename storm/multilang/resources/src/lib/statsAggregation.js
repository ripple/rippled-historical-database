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
    hour : { },
    day  : { },
    week : { }
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
    var bucket;

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
      updateBucket('hour', hour, 'metric', 'accounts_created');
      updateBucket('day',  day,  'metric', 'accounts_created');
      updateBucket('week', week, 'metric', 'accounts_created');
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
          ledger_count      : 0
        }
      }
    }

    if (!self.stats[interval][time].increment) {
      self.stats[interval][time].increment = function (family, column) {
        if (!this[family][column]) {
          this[family][column] = 1;
        } else {
          this[family][column]++
        }
      }
    }

    return self.stats[interval][time];
  }

  function updateBucket(interval, time, family, column) {
    getBucket(interval, time).increment(family, column);
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
