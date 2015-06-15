var config = require('../../config/import.config');
var Server = require('../../api/server');

var hbaseConfig = config.get('hbase');
var prefix = config.get('prefix') || 'TEST_';
var port = config.get('port') || 7111;
var server;

hbaseConfig.prefix = prefix;
hbaseConfig.logLevel = 2;
hbaseConfig.max_sockets = 200;
hbaseConfig.timeout = 30000;

server = new Server({
  postgres: undefined,
  hbase: hbaseConfig,
  port: port
});
