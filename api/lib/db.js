var Knex    = require('knex');
var Promise = require('bluebird');
var log     = require('../../lib/log')('postgres');
var moment  = require('moment');
var sjcl    = require('ripple-lib').sjcl;

var EPOCH_OFFSET = 946684800;
log.level(4);

var SerializedObject = require('ripple-lib').SerializedObject;
var UInt160 = require('ripple-lib').UInt160;

var DB = function(config) {
  var self  = this;
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
  * getTx
  * get transaction for a specific tx_hash
  * @param {Object} options
  * @param {Function} callback
  */ 

  self.getTx = function (options, callback) {
    var txQuery = prepareTxQuery();
    if (txQuery.error) {
      return callback(txQuery);
    }

    txQuery.nodeify(function(err, transactions){
      if (err) return callback(err);
      else handleResponse(transactions[0]);
    });

    function prepareTxQuery(){
      var query = self.knex('transactions')
          .where('transactions.tx_hash', self.knex.raw("decode('"+options.tx_hash+"', 'hex')"))
          .select(self.knex.raw("encode(transactions.tx_raw, 'hex') as tx_raw"))
          .select(self.knex.raw("encode(transactions.tx_meta, 'hex') as tx_meta"))
          .select(self.knex.raw("encode(transactions.ledger_hash, 'hex') as ledger_hash"))
          .select('transactions.ledger_index')
          .select('transactions.executed_time')
          .select('transactions.tx_type');

      return query;
    }

    function handleResponse(transaction) {
      if (!options.binary) {
        transaction.tx = new SerializedObject(transaction.tx_raw).to_json();
        transaction.meta = new SerializedObject(transaction.tx_meta).to_json();
        delete transaction.tx_raw;
        delete transaction.tx_meta;
      }
      callback(null, transaction);
    }
  };

  /**
  * 
  * getLedger
  * get ledger for a specific ledger_index, ledger_hash, or closing_time
  * @param {Object} options
  * @param {Function} callback
  */ 

  self.getLedger = function (options, callback) {

    var ledgerQuery = prepareLedgerQuery();
    if (ledgerQuery.error) {
      return callback(ledgerQuery);
    }

    ledgerQuery.nodeify(function(err, ledgers){
      if (err) return callback(err);
      else if (ledgers.length === 0) callback({error: "No ledgers found.", code:400});
      else if (options.tx_return !== "none") {
        var ledger = parseLedger(ledgers[0]),
            ledger_index = ledger.ledger_index;
            txQuery = prepareTxQuery(ledger_index);
        if (txQuery.error){
          return callback(txQuery);
        }
        txQuery.nodeify(function(err, transactions) {
          if (err) return callback(err);
          else {
            handleResponse(ledger, transactions);
          }
        });
      }
      else {
        callback(null, parseLedger(ledgers[0]) );
      }
    });

    function prepareLedgerQuery() {
      var query = self.knex('ledgers')
        .select(self.knex.raw("encode(ledgers.ledger_hash, 'hex') as ledger_hash"))
        .select('ledger_index')
        .select(self.knex.raw("encode(ledgers.parent_hash, 'hex') as parent_hash"))
        .select('total_coins')
        .select('closing_time')
        .select('close_time_res')
        .select(self.knex.raw("encode(ledgers.accounts_hash, 'hex') as accounts_hash"))
        .select(self.knex.raw("encode(ledgers.transactions_hash, 'hex') as transactions_hash"))
        .orderBy('ledgers.ledger_index', 'desc')
        .orderBy('closing_time', 'desc')
        .limit(1);
      
      if (!options.ledger_index && !options.datetime && !options.ledger_hash) {
        query.where('ledgers.closing_time', '<=', moment().unix());
      }
      else {
        if (options.ledger_index) 
          query.where('ledgers.ledger_index', options.ledger_index);
        if (options.datetime) {
          var iso_datetime = moment.utc(options.datetime, moment.ISO_8601);
          if (iso_datetime.isValid()) {
            query.where('ledgers.closing_time', '<=', iso_datetime.unix());
          }
          else if (!isNaN(options.datetime)) {
            query.where('ledgers.closing_time', '<=', options.datetime);
          }
          else return {error:'invalid datetime, format must be ISO 8601or Unix offset', code:400};
        }
        if (options.ledger_hash)
          query.where('ledgers.ledger_hash', self.knex.raw("decode('"+options.ledger_hash+"', 'hex')"));
      }
      return query;
    }

    function prepareTxQuery(ledger_index) {
      var query = self.knex('transactions')
                  .where('transactions.ledger_index', ledger_index);

      if (options.tx_return === 'hex')
        query.select(self.knex.raw("encode(transactions.tx_hash, 'hex') as tx_hash"));
      else if (options.tx_return === "binary")
        query
          .select(self.knex.raw("encode(transactions.tx_meta, 'hex') as tx_meta"))
          .select(self.knex.raw("encode(transactions.tx_raw, 'hex') as tx_raw"));
      else if (options.tx_return === 'json')
        query.select(self.knex.raw("encode(transactions.tx_raw, 'hex') as tx_raw"))
          .select(self.knex.raw("encode(transactions.tx_meta, 'hex') as tx_meta"));

      return query;
    }

    function handleResponse(ledger, transactions) {
      if (options.tx_return === "hex") {
        var transaction_list = [];
        for (var i=0; i<transactions.length; i++){
          transaction_list.push(transactions[i].tx_hash);
        }
        ledger.transactions = transaction_list;
      }
      else if (options.tx_return === "binary") ledger.transactions = transactions;
      else if (options.tx_return === "json") {
        for (var i=0; i<transactions.length; i++){
          var row = transactions[i];
          row.tx = new SerializedObject(row.tx_raw).to_json();
          row.meta = new SerializedObject(row.tx_meta).to_json();
          delete row.tx_raw;
          delete row.tx_meta;
        }
        ledger.transactions = transactions;
      }
      callback(null, ledger);
    }

    function parseLedger(ledger) {
      ledger.ledger_index   = parseInt(ledger.ledger_index);
      ledger.closing_time   = parseInt(ledger.closing_time);
      ledger.close_time_res = parseInt(ledger.close_time_res);
      ledger.total_coins    = parseInt(ledger.total_coins);
      ledger.close_time     = ledger.closing_time;
      delete ledger.closing_time;
      return ledger;
    }

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
    var result = prepareQuery ();
    if (result.error) {
      callback(result);
      return;
    }
    
    log.debug(new Date().toISOString(), 'getting transactions:', options.account); 
    
    
    //execute the query      
    result.query.nodeify(function(err, rows) {
      log.debug(new Date().toISOString(), (rows ? rows.length : 0) + ' transactions found'); 
            
      if (err) {
        log.error(err);
        callback({error:err, code:500});
        return;
        
      //get a count of all the rows that
      //are found without a limit
      } else if (rows.length) {
        result.count.nodeify(function(err, resp) {
          if (err) {
            log.error(err);
            callback({error:err, code:500});
            return;  
          } 
      
          handleResponse(rows, parseInt(resp[0].count, 10));
        });
        
      } else {
        handleResponse(rows, 0);      
      }
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
      var types;
      var results;
      var count;
      
      var query = self.knex('account_transactions')
        .innerJoin('transactions', 'account_transactions.tx_hash', 'transactions.tx_hash')
        .where('account_transactions.account', options.account)
      
      if (options.offset) {
        query.offset(options.offset || 0); 
      }
  
      //handle start date/time - optional
      if (options.start) {
        start = moment.utc(options.start, moment.ISO_8601);
  
        if (start.isValid()) {
          query.where('account_transactions.executed_time', '>=', start.unix());        
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
      //can be comma separated list
      if (options.result && options.result !== 'all') {
        results = options.result.split(',');
        query.where(function() {
          var q = this;
          results.forEach(function(result) {
            q.orWhere('account_transactions.tx_result', result.trim());   
          });
        });
        
      } else if (!options.result) {
        query.where('account_transactions.tx_result', 'tesSUCCESS');
      } 
      
      //specify a type - optional
      //can be comma separate list
      if (options.type) {
        types = options.type.split(',');
        query.where(function() {
          var q = this;
          types.forEach(function(type) {
            q.orWhere('account_transactions.tx_type', type.trim());   
          });
        });
      }
      
      var count = query.clone();
      count.count();
      
      query.select(self.knex.raw("encode(transactions.tx_raw, 'hex') as tx_raw"))
        .select(self.knex.raw("encode(transactions.tx_meta, 'hex') as tx_meta"))
        .select(self.knex.raw("encode(account_transactions.tx_hash, 'hex') as tx_hash"))      
        .select('account_transactions.ledger_index')
        .select('account_transactions.tx_seq')
        .select('account_transactions.executed_time')
        .orderBy('account_transactions.ledger_index', descending ? 'desc' : 'asc')
        .orderBy('account_transactions.tx_seq', descending ? 'desc' : 'asc')
        .limit(options.limit || 20)
      
      log.debug(query.toString());
      return {
        count : count,
        query : query
      };     
    }
    
   /**
    * handleResponse 
    * @param {Object} rows
    * @param {Object} callback
    */ 
    function handleResponse (rows, total) {
      
      var transactions = [];
      
      //if (options.limit && parseInt(options.limit, 10) < rows.length) {
      //  rows = rows.slice(0, options.length);
      //}

      rows.forEach(function(row) {
        var data = { };
        
        data.hash         = row.tx_hash.toUpperCase();
        data.ledger_index = parseInt(row.ledger_index, 10);
        data.date         = moment.unix(parseInt(row.executed_time, 10)).utc().format();
        
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
        
        
          //NOTE: keeping these here for backwards compatability for
          //the moment, to be removed!
          data.tx.hash          = row.tx_hash.toUpperCase();
          data.tx.ledger_index  = parseInt(row.ledger_index, 10);
          data.tx.executed_time = parseInt(row.executed_time, 10);
          data.tx.date          = data.tx.executed_time - EPOCH_OFFSET;
        }
                
        transactions.push(data);
      });
      
      callback(null, {
        transactions : transactions,
        total        : total || 0
      });
    }
  };
  
  return this;
};

module.exports = DB;