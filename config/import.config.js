var nconf = require('nconf');

nconf.argv()
  .env()
  .file({ file: 'api.config.json' });
  
module.exports = nconf;