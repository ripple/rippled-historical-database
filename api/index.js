var config  = require('../config/api.config');
var Server  = require('./server');
var options = {
  postgres : config.get('postgres'),
  port     : config.get('port')
}

var server = new Server(options);
