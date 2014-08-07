Sequelize = require('sequelize')
pg = require('pg');

var databaseUrl = process.env.DATABASE_URL;
if (!databaseUrl) throw new Error('DATABASE_URL env variable is required');

function DB() {
  var match = databaseUrl.match(/postgres:\/\/([^:]+):([^@]+)@([^:]+):(\d+)\/(.+)/);
  this.db = new Sequelize(match[5], match[1], match[2], {
    dialect: 'postgres',
    protocol: 'postgres',
    port: match[4],
    host: match[3],
    logging: false,
    native: false,
    pool: {
      maxConnections: 10,
      maxIdleTime: 30
    }
  });
}



module.exports = new DB();
