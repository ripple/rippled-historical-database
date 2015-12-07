var nconf = require('nconf');

nconf.argv()
  .env()
  .file({ file: './config/api.config.json' });

module.exports = nconf;
