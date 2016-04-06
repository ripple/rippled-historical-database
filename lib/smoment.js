var moment = require('moment');
var Logger = require('./logger');
var log = new Logger({scope : 'smoment'});

// Simpler moment module specialized for our API

module.exports = function(time) {

  var m = undefined;
  var isoUTC = 'YYYY-MM-DDTHH:mm:ss[Z]';
  var hbaseFmt = 'YYYYMMDDHHmmss';    // Format used on Hbase row keys
  var hbaseFmtMS = 'YYYYMMDDHHmmssSSS';

  var fmts = [
    { pattern: 'YYYY-MM-DDTHH:mm:ss',  granularity: 'second' },
    { pattern: 'YYYY-MM-DDTHH:mm:ssZ',  granularity: 'second' },
    { pattern: 'YYYY-MM-DDTHH:mm:ss.SSSZ',  granularity: 'ms' },
    { pattern: 'YYYY-MM-DDTHH:mm',  granularity: 'minute' },
    { pattern: 'YYYY-MM-DDTHH',  granularity: 'hour' },
    { pattern: 'YYYY-MM-DD',  granularity: 'day' },
    { pattern: 'YYYY-MM',  granularity: 'month' },
    { pattern: 'YYYY',  granularity: 'year' },
    { pattern: hbaseFmt, granularity: 'second' },
    { pattern: hbaseFmtMS, granularity: 'ms' },
  ]

  if(time === undefined) {
    m = { moment: moment.utc(), granularity: 'second' };
  } else if (typeof time === 'number' || /^\d{10}$/.test(time)) {
    m = { moment: moment.unix(time).utc(), granularity: 'second' };
  } else if (typeof time === 'object' && time._isAMomentObject) {
    m = { moment: time.utc(), granularity: 'second' };
  } else if (typeof time === 'object' && time._isASMomentObject) {
    m = { moment: moment(time.moment).utc(), granularity: time.granularity };
  } else {
    fmts.every( function(fmt) {
      var m0 = moment.utc(time, fmt.pattern, true);
      if(m0.isValid()) {
        m = { moment: m0, granularity: fmt.granularity };
        return false;
      }
      return true;
    });
  }

  if(m) {
    m._isASMomentObject = true;

    m.format = function(fmt) {
      return this.moment.utc().format(fmt || isoUTC);
    };

    m.hbaseFormat = function(ms) {
      return this.moment.format(ms ? hbaseFmtMS : hbaseFmt);
    };

    m.hbaseFormatStartRow = function(ms) {
      return this.hbaseFormat(ms);
    };

    m.hbaseFormatStopRow = function (ms) {
      var time = moment(this.moment).add(1, this.granularity);
      return time.format(ms ? hbaseFmtMS : hbaseFmt);
    };

    m.unix = function(ms) {
      return ms ? this.moment.format('x') : this.moment.unix();
    };
  }

  if(!m) {
    log.warn('smoment: could not parse the date: ' + time);
  }
  return m;
};
