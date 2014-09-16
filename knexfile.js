var config = require('./src/config.json')
var obj = {
    integration: {
        client: "postgres",
        connection: config.db
    },
    staging: {
        client: "postgres",
        connection: config.db
    },
    production: {
        client: "postgres",
        connection: config.db
    }
};
// some call it integration
obj.development = obj.integration;
module.exports = exports = obj;
