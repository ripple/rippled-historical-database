var nconf = require('nconf');

nconf.argv()
  .env()
  .file({ file: './config/import.config.json' });
  
module.exports = nconf;