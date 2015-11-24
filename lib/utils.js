var moment = require('moment');
var binary = require('ripple-binary-codec');
var querystring = require('querystring');

/**
 * formatTime
 */

module.exports.formatTime = function(time) {
  if (typeof time === 'number') {
    t = moment.unix(time).utc();
  } else {
    t = moment.utc(time);
  }
  return t.format('YYYYMMDDHHmmss');
};

/**
 * unformatTime
 */

module.exports.unformatTime = function(time) {
  var t = [
    Number(time.slice(0, 4) || 0),     //year
    Number(time.slice(4, 6) || 1) - 1, //month
    Number(time.slice(6, 8) || 0),     //day
    Number(time.slice(8, 10) || 0),    //hour
    Number(time.slice(10, 12) || 0),   //minute
    Number(time.slice(12, 14) || 0),   //second
  ]

  return moment.utc(t);
};

/**
 * reverseTimestamp
 */

module.exports.reverseTimestamp = function(time) {
  time = parseInt(module.exports.formatTime(time), 10);
  return 70000000000000 - time;
};

/**
 * padNumber
 */

module.exports.padNumber = function (num, size) {
  var s = num+"";
  if (!size) size = 10;
  while (s.length < size) s = "0" + s;
  return s;
};

/**
 * formatTime
 * Convert json to binary/hex to store as raw data
 */

module.exports.toHex = function (input) {
  return binary.encode(input);
}

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
      time.subtract(time.month()%multiple, 'months');
    }
  } else if (interval === 'yea') {
    time.startOf('year')
    if (multiple > 1) {
      time.subtract(time.years()%multiple, 'years');
    }
  }

  return time;
}

/**
 * flattenJSON
 */

module.exports.flattenJSON = function(data) {
  var result = {};

  function recurse (cur, prop) {
    if (Object(cur) !== cur) {
        result[prop] = cur;
    } else if (Array.isArray(cur)) {
      for(var i=0, l=cur.length; i<l; i++) {
        recurse(cur[i], prop + '[' + i + ']');
        if (l == 0) {
          result[prop] = [];
        }
      }
    } else {
      var isEmpty = true;
      for (var p in cur) {
        isEmpty = false;
        recurse(cur[p], prop ? prop+"."+p : p);
      }
      if (isEmpty && prop) {
        result[prop] = {};
      }
    }
  }
  recurse(data, "");
  return result;
}

/**
 * unflattenJSON
 */

module.exports.unflattenJSON = function(data) {
  if (Object(data) !== data || Array.isArray(data)) {
    return data;
  }

  var regex = /\.?([^.\[\]]+)|\[(\d+)\]/g;
  var resultholder = {};
  for (var p in data) {
    var cur = resultholder;
    var prop = '';
    var m;
    while (m = regex.exec(p)) {
      cur = cur[prop] || (cur[prop] = (m[2] ? [] : {}));
      prop = m[2] || m[1];
    }

    cur[prop] = data[p];
  }

  return resultholder[''] || resultholder;
};

/**
 * addLinkHeader
 */

module.exports.addLinkHeader = function(req, res, marker) {
  var query = JSON.parse(JSON.stringify(req.query));
  query.marker = marker;
  next = req.protocol + '://' +
    req.get('host') +
    req.path + '?' +
    querystring.unescape(querystring.stringify(query));
  res.links({next: next});
}
