var moment = require('moment');

module.exports.formatTime = function(time) {
  t = moment.unix(time).utc();
  return t.format('YYYYMMDDHHmmss');
}