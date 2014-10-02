var Knex    = require('knex');
var Promise = require('bluebird');
var log     = require('../../lib/log')('postgres');
var moment  = require('moment');

var SerializedObject = require('ripple-lib').SerializedObject;
var UInt160 = require('ripple-lib').UInt160;

var DB = function(config) {
	var self = this;
	var knex = Knex.initialize({
		client     : config.dbtype,
		connection : config.db
	});

 /**
  * 
  * getAccountTransactions
  * get transactions for a specific account
  * @param {Object} options
  * @param {Function} callback
  */  
  self.getAccountTransactions = function (options, callback) {
    log.info("ACCOUNT TX:", options.address); 
    
    var descending = options.descending === false ? false : true;
    var start;
    var end;    
    
    var query = knex('accounts')
      .innerJoin('account_transactions', 'accounts.account_id', 'account_transactions.account_id')
      .innerJoin('transactions', 'account_transactions.tx_id', 'transactions.tx_id')
      .where('accounts.account', options.account)
      .select(knex.raw("encode(transactions.tx_raw, 'hex') as tx_raw"))
      .select(knex.raw("encode(transactions.tx_meta, 'hex') as tx_meta"))
      .select('transactions.ledger_index')
      .select('transactions.tx_seq')
      .select('transactions.executed_time')
      .orderBy('transactions.ledger_index', descending ? 'desc' : 'asc')
      .orderBy('transactions.tx_seq', descending ? 'desc' : 'asc')
      .limit(options.limit || 10)
      .offset(options.offset || 0);   

    if (options.start) {
      start = moment.utc(options.start, moment.ISO_8601);

      if (start.isValid()) {
        query.where('transactions.executed_time', '>=', start.unix())        
      } else {
        return callback({error:'invalid start time, format must be ISO 8601', code:400});
      }
    }
   
    if (options.end) {   
      end = moment.utc(options.end, moment.ISO_8601);
      
      if (end.isValid()) {
        query.where('transactions.executed_time', '<=', end.unix());
      } else {
        return callback({error:'invalid end time, format must be ISO 8601', code:400});
      }
    } 
    
    if (options.type) {
      query.where('transactions.type', options.type);
    }
          
    query.nodeify(function(err, rows) {
      if (err) {
        log.error(err);
        return callback({error:err, code:500});
      }
      
      prepareResponse(rows, callback);
    }); 
    
    var prepareResponse = function (rows, callback) {
      var transactions = [];
      
      rows.forEach(function(row) {
        var data = { };
        
        try {
          data.tx   = new SerializedObject(row.tx_raw).to_json();
          data.meta = new SerializedObject(row.tx_meta).to_json();     
        } catch (e) {
          log.error(e);
          return callback({error:e, code:500});
        }
        
        data.tx.ledger_index  = parseInt(row.ledger_index, 10);
        data.tx.executed_time = parseInt(row.executed_time, 10);  
        transactions.push(data);
      });
      
      log.info('Transactions Found:', transactions.length);
      callback(null, transactions);
    };
  };
  
	return this;
};


module.exports = DB;