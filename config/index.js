var nconf = require('nconf');

nconf.argv()
  .env()
  .file('defaults', __dirname + '/config.json');

module.exports = nconf;
