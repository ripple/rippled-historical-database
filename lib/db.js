var Knex = require('knex');

var DB = function(config) {

  var self = this;
  var knex = Knex.initialize({
    client     : config.get('sql:dbtype'),
    connection : config.get('sql:db')
  });
  
  self.saveLedger = function (ledger, callback) {

  //use knex.transactions  
  //console.log(ledger);
  };
  
  return this;
}; 


module.exports = DB;