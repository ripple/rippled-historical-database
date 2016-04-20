var nconf = require('nconf');

nconf.argv()
  .env()
  .defaults({
    port: 7112,
    hbase: {
      prefix: 'test_',
      servers: [
        {
          host: 'hbase',
          port: 9090
        }
      ]
    },
    'hbase-rest' : {
      prefix: 'test_',
      host: 'hbase',
      port: 8080
    }
  });

module.exports = nconf;
