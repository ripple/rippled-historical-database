var moment = require('moment');

// Simpler moment module specialized for our API

module.exports = function(time) {

  var m = undefined;
  var isoUTC = 'YYYY-MM-DDTHH:mm:ss[Z]';
  var hbaseFmt = 'YYYYMMDDHHmmss';    // Format used on Hbase row keys

  var fmts = [
    { pattern: 'YYYY-MM-DDTHH:mm:ss',  granularity: 'second' },
    { pattern: 'YYYY-MM-DDTHH:mm:ssZ',  granularity: 'second' },
    { pattern: 'YYYY-MM-DDTHH:mm',  granularity: 'minute' },
    { pattern: 'YYYY-MM-DDTHH',  granularity: 'hour' },
    { pattern: 'YYYY-MM-DD',  granularity: 'day' },
    { pattern: 'YYYY-MM',  granularity: 'month' },
    { pattern: 'YYYY',  granularity: 'year' },
    { pattern: hbaseFmt, granularity: 'second' }
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

    m.hbaseFormat = function() {
      return this.moment.format(hbaseFmt);
    };

    m.hbaseFormatStartRow = function() {
      return this.hbaseFormat();
    };

    m.hbaseFormatStopRow = function () {
      var time = moment(this.moment).add(1, this.granularity);
      return time.format(hbaseFmt);
    };
  }

  if(!m) console.log('smoment: could not parse the date: '+time);
  return m;
};
