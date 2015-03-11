var Postgres  = require('./client');
var Validator = require('./validator');
var config    = require('../../config/import.config');
var db        = new Postgres(config.get('postgres'));
var v;

v = new Validator({
  ripple   : config.get('ripple'),
  postgres : config.get('postgres'),
  start    : config.get('startIndex')
});

v.start();
