var Promise = require('bluebird');
var moment  = require('moment');
var Logger  = require('../logger');
var Hbase   = require('../hbase/hbase-client');
var utils   = require('../utils');

/**
 * statsAggregation
 */

function StatsAggregation(options) {
  var self = this;

  options.timeout  = 10000;

  var logOpts = {
    scope : 'stats-aggregation',
    file  : options.logFile,
    level : options.logLevel
  };

  this.log     = new Logger(logOpts);
  this.hbase   = new Hbase(options);
  this.ready   = true;
  this.pending = [ ];
  this.stats   = {
    hour : { },
    day  : { },
    week : { }
  };

  self.aggregate();

  /**
   * purge
   * remove old cached data
   */

  function purge(){

    self.ready = false;
    self.log.debug('purge cached data');

    var hour = moment.utc().startOf('hour').subtract(6, 'hours');
    var day  = moment.utc().startOf('day').subtract(1,  'days');
    var week = moment.utc().startOf('isoWeek').subtract(1, 'week');

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

    self.ready = true;
  }

  /**
   * updateWeekly
   * aggregate daily to update weekly
   */

  function updateWeekly() {
    var times = [];
    var now = moment.utc();
    var time = moment.utc(now).startOf('isoWeek');
    var rowkey = 'week|' + utils.formatTime(time);

    self.log.debug('updating weekly stats');
    while (now.diff(time) > 0) {

      times.push(moment(time));
      time.add(1, 'day');
    }

    Promise.map(times, function(time) {
      return self.hbase.getStats({
        interval: 'day',
        time: time
      });
    }).nodeify(function(err, buckets) {
      var week = { };

      if (err) {
        self.log.error(err);
        return;
      }

      buckets.forEach(function(day) {
        var family;
        var metric;
        var column;

        delete day.time;
        delete day.interval;

        for (family in day) {
          for (metric in day[family]) {
            column = family + ':' + metric;

            if (!week[column]) {
              week[column] = day[family][metric];
            } else {
              week[column] += day[family][metric];
            }
          }
        }
      });

      if (week['metric:ledger_interval']) {
        week['metric:ledger_interval'] /= times.length;
      }

      if (week['metric:tx_per_ledger']) {
        week['metric:tx_per_ledger'] /= times.length;
      }

      self.hbase.putRow('agg_stats', rowkey, week)
      .nodeify(function(err, resp) {
        if (err) {
          self.log.error(err);
        }
      });
    });
  }

  // purge every hour
  this.purge = setInterval(purge, 60 * 60 * 1000);

  // update weekly stats every 5 minutes
  this.updateWeekly = setInterval(updateWeekly, 5 * 60 * 1000);
}

/**
 * aggregate
 * aggregate incoming stats
 */

StatsAggregation.prototype.aggregate = function() {
  var self = this;
  var updated = { };
  var bucketList = { };
  var incoming;

  // console.log(self.pending.length, self.ready);

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

  //determine which buckets we need
  incoming.forEach(function(row) {
    var time = moment.unix(row.data.time).utc();
    var hour = moment.utc(time).startOf('hour').format();
    var day  = moment.utc(time).startOf('day').format();
    //var week = moment.utc(time).startOf('isoWeek').format();

    bucketList['hour|' + hour] = true;
    bucketList['day|'  + day]  = true;
    //bucketList['week|' + week] = true;
  });

  //get any from hbase that arent
  //already present
  Promise.map(Object.keys(bucketList), function(key) {
    var parts = key.split('|');

    if (self.stats[parts[0]][parts[1]]) {
      return Promise.resolve(null);
    }

    return self.hbase.getStatsRow({
      interval : parts[0],
      time     : moment.utc(parts[1]),
    });

  }).nodeify(function(err, resp) {
    if (err) {
      self.log.error(err);

      //try these again
      self.ready   = true;
      self.pending = incoming.concat(self.pending);
      setImmediate(aggregate);
      return;
    }

    //organize the buckets
    resp.forEach(function(row) {
      if (!row) return;

      //add increment function
      row.increment = function (family, column, value) {
        if (typeof value === 'undefined') {
          value = 1;
        }

        if (!this[family][column]) {
          this[family][column]  = value;
        } else {
          this[family][column] += value;
        }
      }

      self.stats[row.interval][row.time] = row;
    });

    //handle incoming stats
    incoming.forEach(function(row) {
      var time = moment.unix(row.data.time).utc();
      var hour = moment.utc(time).startOf('hour').format();
      var day  = moment.utc(time).startOf('day').format();
      var week = moment.utc(time).startOf('isoWeek').format();
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

      } else if (row.label === 'payments_count') {
        updateBucket('hour', hour, 'metric', 'payments_count', row.data.count);
        updateBucket('day',  day,  'metric', 'payments_count', row.data.count);
        updateBucket('week', week, 'metric', 'payments_count', row.data.count);

      } else if (row.label === 'exchanges_count') {
        updateBucket('hour', hour, 'metric', 'exchanges_count', row.data.count);
        updateBucket('day',  day,  'metric', 'exchanges_count', row.data.count);
        updateBucket('week', week, 'metric', 'exchanges_count', row.data.count);

      } else if (row.label === 'accounts_created') {
        updateBucket('hour', hour, 'metric', 'accounts_created', row.data.count);
        updateBucket('day',  day,  'metric', 'accounts_created', row.data.count);
        updateBucket('week', week, 'metric', 'accounts_created', row.data.count);

      } else if (row.label === 'ledger_count') {
        updateBucket('hour', hour, 'metric', 'ledger_count');
        updateBucket('day',  day,  'metric', 'ledger_count');
        updateBucket('week', week, 'metric', 'ledger_count');

        updateBucket('hour', hour, 'metric', 'tx_per_ledger', row.data.tx_count);
        updateBucket('day',  day,  'metric', 'tx_per_ledger', row.data.tx_count);
        updateBucket('week', week, 'metric', 'tx_per_ledger', row.data.tx_count);

        updateBucket('hour', hour, 'metric', 'ledger_interval', time);
        updateBucket('day',  day,  'metric', 'ledger_interval', time);
        updateBucket('week', week, 'metric', 'ledger_interval', time);

      } else {
        self.log.info('unhandled stat:', row.label);
      }
    });

    //send updated stats to hbase
    self.updateStats(updated);

    //ready for the next batch
    self.ready = true;
    setImmediate(aggregate);
  });

  /**
   * aggregate
   * function to call from timeout
   */

  function aggregate () {
    self.aggregate();
  }

  /**
   * updateBucket
   * update the bucket with
   * the provided stat
   */

  function updateBucket(interval, time, family, column, value) {
    var bucket;
    var avg;
    var secs;
    var count;

    bucket = self.stats[interval][time];
    if (!bucket) return;

    if (column === 'tx_per_ledger') {
      count = bucket.metric.tx_per_ledger * (bucket.metric.ledger_count-1);
      avg   = (count+value)/bucket.metric.ledger_count;
      bucket.metric.tx_per_ledger = avg.toPrecision(5);

    } else if (column === 'ledger_interval') {
      secs = value.diff(time, 'seconds');
      avg  = secs/bucket.metric.ledger_count;
      bucket.metric.ledger_interval = avg.toPrecision(5);

    } else {
      bucket.increment(family, column, value);
    }

    updated[interval + '|' + time + '|' + family + '|' + column] = true;
  }
};

/**
 * update
 * add an incoming
 * stat to the queue
 */

StatsAggregation.prototype.update = function (stat) {
  this.pending.push(stat);
};

/**
 * updateStat
 * save updated stat
 * to hbase
 */

StatsAggregation.prototype.updateStats = function(updated) {
  var self   = this;
  var rows   = { };
  var parts;
  var value;
  var rowkey;
  var column;

  for(var key in updated) {
    parts  = key.split('|');
    value  = self.stats[parts[0]][parts[1]][parts[2]][parts[3]];
    rowkey = parts[0] + '|' + utils.formatTime(parts[1]);
    column = parts[2] + ':' + parts[3];

    if (!rows[rowkey]) {
      rows[rowkey] = { };
    }

    rows[rowkey][column] = value;
  }

  self.log.debug(Object.keys(rows));
  self.hbase.putRows('agg_stats', rows)
  .nodeify(function(err, resp) {
    if (err) {
      self.log.error(err);
    }
  });
};

module.exports = StatsAggregation;
