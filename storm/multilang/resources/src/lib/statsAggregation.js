var Promise = require('bluebird');
var moment  = require('moment');
var Logger  = require('./modules/logger');
var Hbase   = require('./hbase-client');
var utils   = require('./utils');
var bolt;

/**
 * statsAggregation
 */

function StatsAggregation(options) {
  var self = this;

  options.hbase.logLevel = options.logLevel;
  options.hbase.logFile  = options.logFile;
  options.hbase.timeout  = 10000;

  var logOpts = {
    scope : 'stats-aggregation',
    file  : options.logFile,
    level : options.logLevel
  };

  this.log     = new Logger(logOpts);
  this.hbase   = new Hbase(options.hbase);
  this.ready   = true;
  this.pending = [ ];
  this.stats   = {
    hour : { },
    day  : { },
    week : { }
  };

  self.aggregate();

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

    self.ready = true;

  }, 60 * 60 * 1000);
}

/**
 * aggregate
 * aggregate incoming stats
 */

StatsAggregation.prototype.aggregate = function () {
  var self       = this;
  var updated    = { };
  var bucketList = { };
  var incoming;

  //console.log(self.pending.length, self.ready);

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
    var week = moment.utc(time).startOf('week').format();
    bucketList['hour|' + hour] = true;
    bucketList['day|'  + day]  = true;
    bucketList['week|' + week] = true;
  });

  //get any from hbase that arent
  //already present
  Promise.map(Object.keys(bucketList), function(key) {
    var parts = key.split('|');

    if (self.stats[parts[0]][parts[1]]) {
      return Promise.resolve(null);
    }

    return self.hbase.getStats({
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
      var week = moment.utc(time).startOf('week').format();
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

        updateBucket('hour', hour, 'metric', 'tx_per_ledger', row.data.tx_count);
        updateBucket('day',  day,  'metric', 'tx_per_ledger', row.data.tx_count);
        updateBucket('week', week, 'metric', 'tx_per_ledger', row.data.tx_count);

        updateBucket('hour', hour, 'metric', 'ledger_interval', time);
        updateBucket('day',  day,  'metric', 'ledger_interval', time);
        updateBucket('week', week, 'metric', 'ledger_interval', time);
      }
    });

    //save updated stats
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
