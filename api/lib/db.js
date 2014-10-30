var Knex    = require('knex');
var Promise = require('bluebird');
var log     = require('../../lib/log')('postgres');
var moment  = require('moment');

log.level(4);

var SerializedObject = require('ripple-lib').SerializedObject;
var UInt160 = require('ripple-lib').UInt160;

var DB = function(config) {
	var self = this;
	self.knex = Knex.initialize({
		client     : config.dbtype,
		connection : config.db
	});

 /**
  * migrate
  * run latest db migrations
  */
  self.migrate = function () {
    return self.knex.migrate.latest()
    .spread(function(batchNo, list) {
      if (list.length === 0) {
        log.info('Migration: up to date');
      } else {
        log.info('Migration: batch ' + batchNo + ' run: ' + list.length + ' migrations \n' + list.join('\n'));
      }
    });
  };
  
 /**
  * 
  * getAccountTransactions
  * get transactions for a specific account
  * @param {Object} options
  * @param {Function} callback
  */  
  self.getAccountTransactions = function (options, callback) {
    
    //prepare the sql query
    var query = prepareQuery ();
    if (query.error) {
      return callback(query);
    }
    
    log.debug(new Date().toISOString(), 'getting transactions:', options.account); 
    
    //execute the query      
    query.nodeify(function(err, rows) {
      log.debug(new Date().toISOString(), (rows ? rows.length : 0) + ' transactions found'); 
      
      if (err) {
        log.error(err);
        return callback({error:err, code:500});
      }
      
      handleResponse(rows);
    }); 
    
   /**
    * prepareQuery
    * parse incoming options to create
    * the knex SQL query 
    */
    function prepareQuery () {
      var descending = options.descending === false ? false : true;
      var start;
      var end;
      
      var query = self.knex('account_transactions')
        .innerJoin('transactions', 'account_transactions.tx_hash', 'transactions.tx_hash')
        .where('account_transactions.account', options.account)
        .select(self.knex.raw("encode(transactions.tx_raw, 'hex') as tx_raw"))
        .select(self.knex.raw("encode(transactions.tx_meta, 'hex') as tx_meta"))
        .select('account_transactions.ledger_index')
        .select('account_transactions.tx_seq')
        .select('account_transactions.executed_time')
        .orderBy('account_transactions.ledger_index', descending ? 'desc' : 'asc')
        .orderBy('account_transactions.tx_seq', descending ? 'desc' : 'asc')
        .limit(options.limit || 20)
      
      if (options.offset) {
        query.offset(options.offset || 0); 
      }
  
      //handle start date/time - optional
      if (options.start) {
        start = moment.utc(options.start, moment.ISO_8601);
  
        if (start.isValid()) {
          query.where('account_transactions.executed_time', '>=', start.unix())        
        } else {
          return {error:'invalid start time, format must be ISO 8601', code:400};
        }
      }
     
      //handle end date/time - optional
      if (options.end) {   
        end = moment.utc(options.end, moment.ISO_8601);
        
        if (end.isValid()) {
          query.where('account_transactions.executed_time', '<=', end.unix());
        } else {
          return {error:'invalid end time, format must be ISO 8601', code:400};
        }
      } 
      
      //handle minLedger - optional
      if (options.minLedger) {
        query.where('account_transactions.ledger_index', '>=', options.minLedger);        
      }
     
       //handle maxLedger - optional
      if (options.maxLedger) {
        query.where('account_transactions.ledger_index', '<=', options.maxLedger);        
      }            
      
      //specify a result - default to tesSUCCESS,
      //exclude the where if 'all' is specified
      if (options.result && options.result !== 'all') {
        query.where('account_transactions.tx_result', options.result);
        
      } else if (!options.result) {
        query.where('account_transactions.tx_result', 'tesSUCCESS');
      } 
      
      //specify a type - optional
      if (options.type) {
        query.where('account_transactions.tx_type', options.type);  
      }
      
      log.debug(query.toSQL().sql);
      return query;     
    }
    
   /**
    * handleResponse 
    * @param {Object} rows
    * @param {Object} callback
    */ 
    function handleResponse (rows) {
      var transactions = [];
      
      //if (options.limit && parseInt(options.limit, 10) < rows.length) {
      //  rows = rows.slice(0, options.length);
      //}

      rows.forEach(function(row) {
        var data = { };
        
        if (options.binary) {
          data.tx   = row.tx_raw;
          data.meta = row.tx_meta;
          
        } else {
          try {
            data.tx   = new SerializedObject(row.tx_raw).to_json();
            data.meta = new SerializedObject(row.tx_meta).to_json();     
          } catch (e) {
            log.error(e);
            return callback({error:e, code:500});
          }          
        }
        
        data.tx.ledger_index  = parseInt(row.ledger_index, 10);
        data.tx.executed_time = parseInt(row.executed_time, 10);  
        transactions.push(data);
      });
      
      callback(null, transactions);
    };
  };
  
	return this;
};


module.exports = DB;