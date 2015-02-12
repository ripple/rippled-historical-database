var config  = require('../../config/import.config');
var Logger  = require('../../storm/multilang/resources/src/lib/modules/logger');
var utils   = require('../../storm/multilang/resources/src/lib/utils.js');
var couchdb = require('../../import/couchdb/client');
var moment  = require('moment');

var log = new Logger({
  scope : 'cache-exchange',
  level : config.get('logLevel') || 0,
  file  : config.get('logFile')
});

var intervals = {
  minute : [1,5,15,30],
  hour   : [1,2,4],
  day    : [1,3,7],
  month  : [1,3],
  year   : [1]
}

var Exchanges = function (hbase) {
  var self = this;
  
  self.cacheIntervals = function (key, time) {
    cacheIntervals(key, 'minute', time);
    cacheIntervals(key, 'hour',   time);
    cacheIntervals(key, 'day',    time);
    cacheIntervals(key, 'month',  time);
  };
  
  /*
  self.initTables = function () {
    var HBaseRest = require('hbase');
    var rest      = HBaseRest(config.get('hbase-rest'));
    
    for (var key in intervals) {
      intervals[key].forEach(function(multiple) {
        var table = 'agg_exchange_' + multiple + key;
        console.log(table);
        rest.getTable(table)
        .create({ColumnSchema : [{name:'f'},{name:'d'}]}, function(err, resp){ 
          console.log(err, resp);
        }); 
      }); 
    }  
  };
  
  //self.initTables();
  */


  /**
   * cache Intervals
   * cache all intervals for a given 
   * time and time unit
   */

  function cacheIntervals (key, interval, first, last) {
    var options = { };
    var start;
    var end;
    var num;
    var max;

    switch (interval) {
      case 'minute' : 
        options.group_level = 6;
        num = 30;
        break;
      case 'hour' : 
        options.group_level = 5;
        num = 4;
        break;
      case 'day' : 
        options.group_level = 4;
        num = 7;
        break;
      case 'month' : 
        options.group_level = 3;
        num = 3;
        break;
      default : 
        options.group_level = 2;
        num = 1;
        break;
    }

    start = utils.getAlignedTime(first, interval, num);
    end   = utils.getAlignedTime(last || first, interval, num).add(num, interval);

    options.startkey = [key].concat(start.toArray().slice(0,6));
    options.endkey   = [key].concat(end.toArray().slice(0,6));

    couchdb.nano.view("offersExercisedV3", "v2", options, function (err, resp){
      var row;

      if (err || !resp || !resp.rows || !resp.rows.length) {
        console.log(err);
        log.error("couchdb:", err, resp);
        return;
      }

      intervals[interval].forEach(function(multiple) {
        console.log(key, interval, multiple);
        rows = groupIntervals(resp.rows, interval, multiple);
        console.log(rows.length, rows[0].start_time);

        saveIntervals({
          key      : key,
          unit     : interval, 
          multiple : multiple
        }, rows);
      });
    });
  };

  /**
   * groupIntervals
   * create a multiple interval aggregation
   */

  function groupIntervals (rows, interval, multiple) {
    var results = [];
    var index;
    var reduced;
    var now;
    var time;

    time = moment.utc(rows[0].key.slice(1));
    time = utils.getAlignedTime(time, interval, multiple);

    //couchdb views store the exchange rate 
    //inversed, so we need to invert again, and 
    //swap the hogh and low
    function addResult (reduced) {
      results.push({
        start_time     : reduced.startTime, 
        base_volume    : reduced.curr1Volume, 
        counter_volume : reduced.curr2Volume, 
        count          : reduced.numTrades, 
        open           : 1 / reduced.open,
        high           : 1 / reduced.low, 
        low            : 1 / reduced.high,  
        close          : 1 / reduced.close, 
        vwap           : reduced.curr2Volume / reduced.curr1Volume,
        open_time      : moment.utc(reduced.openTime).format(),
        close_time     : moment.utc(reduced.closeTime).format()
      });      
    }

    rows.forEach(function(row){

      //if this is the first row, or the 
      //open time exceeds epoch time, start a 
      //new aggregated bin 
      if (time.diff(row.value.openTime) <= 0 || !reduced) {

        if (reduced) addResult(reduced);

        //copy this row as the first aggregated bin
        reduced = JSON.parse(JSON.stringify(row.value));

        //advance to the next epoch, and set the start time
        while(time.diff(reduced.openTime) <= 0) {
          reduced.startTime = time.format();
          time.add(multiple, interval); //end of epoch 
        } 

      //merge this row with the 
      //current aggregated row  
      } else {
        if (row.value.openTime<reduced.openTime) {
          reduced.openTime = row.value.openTime;
          reduced.open     = row.value.open;
        }

        if (reduced.closeTime<row.value.closeTime) {
          reduced.closeTime = row.value.closeTime;
          reduced.close     = row.value.close;
        }

        if (row.value.high>reduced.high) reduced.high = row.value.high;
        if (row.value.low<reduced.low)   reduced.low  = row.value.low;

        reduced.curr1Volume += row.value.curr1Volume;
        reduced.curr2Volume += row.value.curr2Volume;
        reduced.numTrades   += row.value.numTrades;
      }
    });

    addResult(reduced); //add the last row
    return results;     

  };

  /**
   * saveInterval
   * save an aggregated interval to hbase
   */

  function saveIntervals (options, rows) {
    var columns = [];
    var base;
    var counter;
    var keyBase;
    var rowKey;
    var table;

    pair    = options.key.split(':');
    base    = pair[0].split('.');
    counter = pair[1].split('.');

    keyBase = base[0] + '|' + (base[1] || '') + '|' +
      counter[0] + '|' + (counter[1] || '');

    table = 'agg_exchange_' + options.multiple + options.unit;

    rows.forEach(function(row) {
      var rowKey = keyBase + '|' + utils.formatTime(row.start_time);

      hbase.putRow(table, rowKey, row)
      .nodeify(function(err, resp) {
        if (err) {
          log.error('error saving to hbase: ', err);
        }
      });
    });
  }
}

module.exports = function (hbase) {
  return new Exchanges(hbase);
}
