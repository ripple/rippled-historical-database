var config = require('./config.json');
var Knex   = require('knex');
var knex   = Knex.initialize({
    client: 'postgres',
    connection : config.db
});

var db = { };

db.saveLedger = function (ledger, callback) {

  //use knex.transactions  
  console.log(ledger);
}; 


module.exports = db;