var moment = require('moment');

module.exports.formatTime = function(time) {
  if (typeof time === 'number') {
    t = moment.unix(time).utc();
  } else {
    t = moment.utc(time);
  }
  return t.format('YYYYMMDDHHmmss');
};

/*
 * getAlignedTime - uses the interval and multiple
 * to align the time to a consistent series, such as 9:00, 9:05, 9:10...
 * rather than 9:03, 9:08, 9:13...
 */ 

module.exports.getAlignedTime = function (original, increment, multiple) {
  var time = moment.utc(original); //clone the original
  if (!multiple) multiple = 1;
  
  increment = increment ? increment.slice(0,3) : null;

  if (increment === 'sec') {
    time.subtract({
      ms      : time.milliseconds(), 
      seconds : multiple === 1 ? 0 : time.seconds()%multiple
    });   
    
  } else if (increment === 'min') {
    time.subtract({
      ms      : time.milliseconds(), 
      seconds : time.seconds(), 
      minutes : multiple === 1 ? 0 : time.minutes()%multiple
    });
          
  } else if (increment === 'hou') {
    time.subtract({
      ms      : time.milliseconds(), 
      seconds : time.seconds(), 
      minutes : time.minutes(),
      hours   : multiple === 1 ? 0 : time.hours()%multiple
    });   
           
  } else if (increment === 'day') {
    var days;
    var diff;
    
    if (multiple === 1) {
      days = 0;
      
    } else { 
      diff = time.diff(moment.utc([2013,0,1]), 'hours')/24;
      if (diff<0) days = multiple - (0 - Math.floor(diff))%multiple;
      else days = Math.floor(diff)%multiple;
    }
    
    time.subtract({
      ms      : time.milliseconds(), 
      seconds : time.seconds(), 
      minutes : time.minutes(),
      hours   : time.hours(),
      days    : days
    }); 

  } else if (increment === 'mon') {
    time.subtract({
      ms      : time.milliseconds(), 
      seconds : time.seconds(), 
      minutes : time.minutes(),
      hours   : time.hours(),
      days    : time.date()-1,
      months  : multiple === 1 ? 0 : time.months()%multiple
    }); 
  } else if (increment === 'yea') {
    time.subtract({
      ms      : time.milliseconds(), 
      seconds : time.seconds(), 
      minutes : time.minutes(),
      hours   : time.hours(),
      days    : time.date()-1,
      months  : time.months(),
      years   : multiple === 1 ? 0 : time.years()%multiple
    }); 
  }
  
  return time;    
}