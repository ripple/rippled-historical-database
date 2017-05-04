var nconf = require('nconf');

nconf.argv()
  .env()
  .defaults({
    port: 7112,
    prefix: '',
    hbase: {
      servers: [
        {
          host: 'hbase',
          port: 9090
        }
      ]
    },
    'hbase-rest': {
      host: 'hbase',
      port: 8080
    },
    ripple: {
      server: 'wss://s1.ripple.com:443'
    },
    'validator-domains': {
      'n9LYyd8eUVd54NQQWPAJRFPM1bghJjaf1rkdji2haF4zVjeAPjT2': 'ripple.com'
    }
  });

module.exports = nconf;
