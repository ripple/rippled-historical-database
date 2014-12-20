var log     = require('../log')('cache-exchange');
var utils   = require('../utils');
var moment  = require('moment');
var couchdb = require('../../import/couchdb/client');
var hbase   = require('../../import/hbase/client');

var intervals = {
  minute : [1,5,15,30],
  hour   : [1,2,4],
  day    : [1,3,7],
  month  : [1,3]
}

var Exchanges = function () {
  var self = this;
  
  self.cacheIntervals = function (key, time) {
    cacheIntervals(key, 'minute', time);
    cacheIntervals(key, 'hour',   time);
    cacheIntervals(key, 'day',    time);
    cacheIntervals(key, 'month',  time);
  };
  
  self.initTables = function () {
    for (var key in intervals) {
      intervals[key].forEach(function(multiple) {
        var table = 'agg_exchange_' + multiple + key;
        console.log(table);
        hbase.hbase.getTable(table)
        .create({ColumnSchema : [{name:'f'},{name:'m'}]}, function(err, resp){ 
          console.log(err, resp);
        }); 
      }); 
    }  
  };
};

/**
 * cache Intervals
 * cache all intervals for a given 
 * time and time unit
 */

function cacheIntervals (key, interval, time) {
  var start   = utils.getAlignedTime(time, interval, 30);
  var end     = moment.utc(start).add(30, interval);

  var options = { 
    startkey    : [key].concat(start.toArray().slice(0,6)),
    endkey      : [key].concat(end.toArray().slice(0,6)),
  };

  switch (interval) {
    case 'minute' : 
      options.group_level = 6;
      break;
    case 'hour' : 
      options.group_level = 5;
      break;
    case 'day' : 
      options.group_level = 4;
      break;
    case 'month' : 
      options.group_level = 3;
      break;
    default : 
      options.group_level = 2;
      break;
  }

  couchdb.nano.view("offersExercisedV3", "v2", options, function (err, resp){
    var row;

    if (err || !resp || !resp.rows || !resp.rows.length) {
      log.error(err, resp);
      return;
    }

    intervals[interval].forEach(function(multiple) {
      row = groupIntervals(resp.rows, time, interval, multiple);
      console.log(key, interval, multiple);
      if (!row) {
        console.log(resp.rows);
      }
      saveInterval(key, {unit:interval,multiple:multiple}, row);
    });
  });
};

/**
 * groupIntervals
 * create a multiple interval aggregation
 */
 
function groupIntervals (rows, time, interval, multiple) {
  var results = [];
  var row;
  var index;
  var reduced;
  var now;
  
  time = utils.getAlignedTime(time, interval, multiple);
  
  if (multiple === 1) {
    index = rows.length;
    
    while(index--) {
      if (time.unix() === moment.utc(rows[index].key.slice(1)).unix()) {
        row = rows[index];
        break;
      }
    }
    
    if (!row) {
      log.error('row not found', time.format());
      console.log(rows);
      return null;
    }
    
    return {
      start_time     : time.format(),
      base_volume    : row.value.curr2Volume,
      counter_volume : row.value.curr1Volume,
      count          : row.value.numTrades,
      open           : row.value.open,
      high           : row.value.high,
      low            : row.value.low,
      close          : row.value.close,
      vwap           : row.value.curr1Volume / row.value.curr2Volume,
      open_time      : moment.utc(row.value.openTime).format(),  //open  time
      close_time     : moment.utc(row.value.closeTime).format(), //close time
    }; 
    
  } else {

    for (var i=0; i<rows.length; i++) {
      if (time.diff(moment.utc(rows[i].key.slice(1)))<=0) {  
        index = i;
        break;
      }
    }
    
    if (typeof index === 'undefined') {
      log.error('no data for row:', time.format(), interval, multiple);
      return null;      
    }
    
    rows = rows.slice(index);
    
    var addResult = function addResult (reduced) {
      results.push({
        start_time     : reduced.startTime, 
        base_volume    : reduced.curr2Volume, 
        counter_volume : reduced.curr1Volume, 
        count          : reduced.numTrades, 
        open           : reduced.open,
        high           : reduced.high, 
        low            : reduced.low,  
        close          : reduced.close, 
        vwap           : reduced.curr1Volume / reduced.curr2Volume,
        open_time      : moment.utc(reduced.openTime).format(),
        close_time     : moment.utc(reduced.closeTime).format()
      });      
    }

    rows.forEach(function(row){

      //if the epoch end time is less than or equal
      //to the open time of the segment, start a new row
      //its possible that the first could have a time
      //diff of 0, we want to accept that as well         
      if (time.diff(row.value.openTime) < 0 || !reduced) {

        //this is the complete row, add it to results
        if (reduced) addResult(reduced);

        //set this row as the first, and advance the
        //epoch tracker past this interval
        reduced = row.value;

        while(time.diff(reduced.openTime) <= 0) {
          reduced.startTime = time.format();
          time.add(multiple, interval); //end of epoch 
        } 

        return;
      }

      //merge this data with the previous rows
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
    });

    addResult(reduced); //add the last row
    return results[0];     
  }
};

/**
 * saveInterval
 * save an aggregated interval to hbase
 */
      
function saveInterval (pair, interval, data) {
  var columns = [];
  var base;
  var counter;
  var rowKey;
  var table;
  
  pair    = pair.split(':');
  base    = pair[0].split('.');
  counter = pair[1].split('.');
  
  rowKey  = base[0] + '|' + (base[1] || '') + '|' +
    counter[0] + '|' + (counter[1] || '') + '|' +
    utils.formatTime(data.start_time);

  table = 'agg_exchange_' + interval.multiple + interval.unit;
  
  for (key in data) {
    columns.push({
      family : 'm',
      name   : key,
      value  : data[key],
    });
  }
  
  hbase.saveRow(table, rowKey, columns)
  .nodeify(function(err, resp) {
    console.log(err, resp);
  });
}

module.exports = new Exchanges();
