var config  = require('../config/api.config');
var Server  = require('./server');
var options = {
  hbase: config.get('hbase'),
  ripple: config.get('ripple'),
  port: config.get('port')
};

var server = new Server(options);
