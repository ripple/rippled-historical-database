var moment = require('moment');

module.exports.formatTime = function(time) {
  if (typeof time === 'number') {
    t = moment.unix(time).utc();
  } else {
    t = moment.utc(time);
  }
  return t.format('YYYYMMDDHHmmss');
};

module.exports.reverseTimestamp = function(time) {
  if (typeof time === 'number') {
    t = moment.unix(time).utc();
  } else {
    t = moment.utc(time);
  }
  return 10000000000 - t.unix();
};

/*
 * getAlignedTime - uses the interval and multiple
 * to align the time to a consistent series, such as 9:00, 9:05, 9:10...
 * rather than 9:03, 9:08, 9:13...
 */ 

module.exports.getAlignedTime = function (original, interval, multiple) {
  var time = moment.utc(original); //clone the original
  if (!multiple) multiple = 1;
  
  interval = interval ? interval.slice(0,3) : null;
  
  if (interval === 'day' && multiple === 7) {
    interval = 'wee';
    multiple = 1;
  }
  
  if (interval === 'sec') {
    time.startOf('second');
    if (multiple > 1) {
      time.subtract(time.seconds()%multiple, 'seconds');       
    }
    
  } else if (interval === 'min') {
    time.startOf('minute');
    if (multiple > 1) {
      time.subtract(time.minutes()%multiple, 'minutes');       
    }
          
  } else if (interval === 'hou') {
    time.startOf('hour');
    if (multiple > 1) {
      time.subtract(time.hours()%multiple, 'hours');       
    }
           
  } else if (interval === 'day') {
    var days;
    var diff;
    
    if (multiple === 1) {
      days = 0;
      
    } else { 
      diff = time.diff(moment.utc([2013,0,1]), 'hours')/24;
      if (diff<0) days = multiple - (0 - Math.floor(diff))%multiple;
      else days = Math.floor(diff)%multiple;
    }
    
    time.startOf('day');
    if (days) {
      time.subtract(days, 'days');     
    }

  } else if (interval === 'wee') {
    time.startOf('isoWeek');
    if (multiple > 1) {
      time.subtract(time.weeks()%multiple, 'weeks');
    }
    
  } else if (interval === 'mon') {
    time.startOf('month');
    if (multiple > 1) {
      time.subtract(time.months()%multiple, 'months'); 
    }
  } else if (interval === 'yea') {
    time.startOf('year')
    if (multiple > 1) {
      time.subtract(time.years()%multiple, 'years');
    }
  }
  
  return time;    
}