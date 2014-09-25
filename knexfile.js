var config = require('./config/import.config.json')
var obj = {
    integration: {
        client: config.sql.dbtype,
        connection: config.sql.db
    },
    staging: {
        client: config.sql.dbtype,
        connection: config.sql.db
    },
    production: {
        client: config.sql.dbtype,
        connection: config.sql.db
    }
};
// some call it integration
obj.development = obj.integration;
module.exports = exports = obj;
