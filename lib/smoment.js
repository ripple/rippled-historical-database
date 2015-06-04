var moment           = require('moment');

// Simpler moment module specialized for our API

module.exports = function(time) {
  
  var m = undefined;
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
  } else if (typeof time === 'number') {
    m = { moment: moment.unix(time).utc(), granularity: 'second' };
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
  	m.format = function(fmt) {
  	  if(fmt === undefined) { return this.moment.format('YYYY-MM-DDTHH:mm:ss'); }
      else { return this.moment.format(fmt); };
  	};
    m.hbaseFormat = function() { return this.moment.format('YYYYMMDDHHmmss'); };           // Replaces formatTime
    m.hbaseFormatStartRow = function() { return this.hbaseFormat(); };
    m.hbaseFormatStopRow = function () { 
      return this.moment.add(1, this.granularity).format(hbaseFmt); 
    };
  }

  if(!m) console.log('smoment: could not parse the date: '+time);
  return m;
};