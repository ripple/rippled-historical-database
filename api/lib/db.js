var Knex    = require('knex');
var Promise = require('bluebird');
var moment  = require('moment');
var sjcl    = require('sjcl');
var Logger  = require('../../lib/logger');
var log     = new Logger({scope : 'postgres'});

var EPOCH_OFFSET = 946684800;
log.level(3);

var binary  = require('ripple-binary-codec');
var UInt160 = require('ripple-lib')._DEPRECATED.UInt160;

var DB = function(config) {
  var self  = this;
  self.knex = Knex.initialize({
      client     : 'postgres',
      connection : config
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
          .select('transactions.ledger_index')
          .select('transactions.executed_time as date')
          .select(self.knex.raw("encode(transactions.tx_hash, 'hex') as hash"))
          .select(self.knex.raw("encode(transactions.tx_raw, 'hex') as tx"))
          .select(self.knex.raw("encode(transactions.tx_meta, 'hex') as meta"));

      log.debug(query.toString());
      return query;
    }

    function handleResponse(transaction) {

      if (!transaction) {
        callback({error:'transaction not found', code:404});
        return;
      }

      transaction.hash = transaction.hash.toUpperCase();
      transaction.ledger_index = Number(transaction.ledger_index);
      transaction.date = moment.unix(transaction.date).utc().format();

      if (!options.binary) {
        try {
          transaction.tx   = binary.decode(transaction.tx.toUpperCase());
          transaction.meta = binary.decode(transaction.meta.toUpperCase());
        } catch (e) {
          log.error('serialization error:', e.toString());
          callback({error:e, code:500});
          return;
        }
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
    var ledger;
    var ledger_index;
    var txQuery;

    if (ledgerQuery.error) {
      return callback(ledgerQuery);
    }

    ledgerQuery.nodeify(function(err, ledgers){
      if (err) return callback(err);
      else if (ledgers.length === 0) callback({error: "ledger not found", code:404});
      else {
        ledger = parseLedger(ledgers[0]);
        ledger_index = ledger.ledger_index;

        if (options.tx_return !== "none") {
          txQuery = prepareTxQuery(ledger_index);
          txQuery.nodeify(function(err, transactions) {
            if (err) return callback(err);
            else handleResponse(ledger, transactions);
          });
        }
        else callback(null, ledger);
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
        .limit(1);

      if (!options.ledger_index && !options.date && !options.ledger_hash) {
        query.where('ledgers.closing_time', '<=', moment().unix());

      } else {
        if (options.ledger_index)
          query.where('ledgers.ledger_index', options.ledger_index);
        if (options.date) {
          var iso_date = moment.utc(options.date, moment.ISO_8601);
          if (iso_date.isValid()) {
            query.where('ledgers.closing_time', '<=', iso_date.unix());
          }
          else if (/^\d+$/.test(options.date)) {
            query.where('ledgers.closing_time', '<=', options.date);
          }
          else return {error:'invalid date, format must be ISO 8601or Unix offset', code:400};
        }
        if (options.ledger_hash)
          query.where('ledgers.ledger_hash', self.knex.raw("decode('"+options.ledger_hash+"', 'hex')"));
      }

      log.debug(query.toString());
      return query;
    }

    function prepareTxQuery(ledger_index) {
      var query = self.knex('transactions')
                  .where('transactions.ledger_index', ledger_index)
                  .select(self.knex.raw("encode(transactions.tx_hash, 'hex') as hash"));

      if (options.tx_return === "binary" || options.tx_return === 'json')
        query
          .select('transactions.executed_time as date')
          .select('transactions.ledger_index')
          .select(self.knex.raw("encode(transactions.tx_raw, 'hex') as tx"))
          .select(self.knex.raw("encode(transactions.tx_meta, 'hex') as meta"));

      log.debug(query.toString());
      return query;
    }

    function handleResponse(ledger, transactions) {
      if (options.tx_return === "hex") {
        var transaction_list = [];
        for (var i=0; i<transactions.length; i++){
          transaction_list.push(transactions[i].hash);
        }
        ledger.transactions = transaction_list;
      }
      else {
        for (var i=0; i<transactions.length; i++){
          var row          = transactions[i];
          row.ledger_index = Number(ledger.ledger_index);
          row.date         = moment.unix(row.date).utc().format();

          if (options.tx_return === "json") {
            try {
              row.tx   = binary.decode(row.tx.toUpperCase());
              row.meta = binary.decode(row.meta.toUpperCase());
            } catch(e) {

              log.error('serialization error:', e.toString());
              callback({error:e, code:500});
              return;
            }
          }
        }
        ledger.transactions = transactions;
      }
      callback(null, ledger);
    }

    function parseLedger(ledger) {
      ledger.ledger_index     = parseInt(ledger.ledger_index);
      ledger.closing_time     = parseInt(ledger.closing_time);
      ledger.close_time_res   = parseInt(ledger.close_time_res);
      ledger.total_coins      = parseInt(ledger.total_coins);
      ledger.close_time       = ledger.closing_time;
      ledger.close_time_human = moment.unix(ledger.close_time).utc().format();
      delete ledger.closing_time;
      return ledger;
    }
  };


  self.getAccountTxSeq = function (options, callback) {
    var query = prepareQuery();
    if (query.error) {
      callback(query);
      return;
    }

    if (options.sequence)
      log.debug(new Date().toISOString(), 'getting transaction:', options.account, options.sequence);
    else
      log.debug(new Date().toISOString(), 'getting transactions:', options.account);

    //execute the query
    query.nodeify(function(err, resp) {

      if (err) {
        log.error(err.toString());
        callback({error:err, code:500});

      } else if (options.count) {
        log.debug(new Date().toISOString(), (resp[0].count || 0) + ' transaction(s) found');
        callback(null, parseInt(resp[0].count, 10));

      } else {
        log.debug(new Date().toISOString(), (resp ? resp.length : 0) + ' transaction(s) found');
        handleResponse(resp);
      }
    });

    function prepareQuery () {
      var descending = options.descending === false ? false : true;
      var types;
      var results;
      var results;

      var query = self.knex('transactions')
        .where('transactions.account', options.account);

      if (options.sequence)
        query.where('transactions.account_seq', options.sequence);
      else {

        //handle min_sequence - optional
        if (options.min_sequence) {
          query.where('transactions.account_seq', '>=', options.min_sequence);
        }

        //handle max_sequence - optional
        if (options.max_sequence) {
          query.where('transactions.account_seq', '<=', options.max_sequence);
        }

        //specify a result - default to tesSUCCESS,
        //exclude the where if 'all' is specified
        //can be comma separated list
        if (options.result && options.result !== 'all') {
          results = options.result.split(',');
          query.where(function() {
            var q = this;
            results.forEach(function(result) {
              q.orWhere('transactions.tx_result', result.trim());
            });
          });

        } else if (!options.result) {
          query.where('transactions.tx_result', 'tesSUCCESS');
        }

        //specify a type - optional
        //can be comma separate list
        if (options.type) {
          types = options.type.split(',');
          query.where(function() {
            var q = this;
            types.forEach(function(type) {
              q.orWhere('transactions.tx_type', type.trim());
            });
          });
        }
      }

      if (options.count) {
        query.count();

      } else {

        query.select(self.knex.raw("encode(transactions.tx_hash, 'hex') as hash"))
          .select('transactions.ledger_index')
          .select('transactions.executed_time')
          .select(self.knex.raw("encode(transactions.tx_raw, 'hex') as tx"))
          .select(self.knex.raw("encode(transactions.tx_meta, 'hex') as meta"))
          .orderBy('transactions.account_seq', descending ? 'desc' : 'asc')
          .limit(options.limit || 20);

        if (options.offset) {
          query.offset(options.offset || 0);
        }
      }

      log.debug(query.toString());
      return query;
    }

       /**
    * handleResponse
    * @param {Object} rows
    * @param {Object} callback
    */
    function handleResponse (rows) {

      var transactions = [];

      rows.forEach(function(row) {
        var data = { };

        data.hash         = row.hash.toUpperCase();
        data.ledger_index = parseInt(row.ledger_index, 10);
        data.date         = moment.unix(parseInt(row.executed_time, 10)).utc().format();

        if (options.binary) {
          data.tx   = row.tx;
          data.meta = row.meta;

        } else {
          try {
            data.tx   = binary.decode(row.tx.toUpperCase());
            data.meta = binary.decode(row.meta.toUpperCase());

          } catch (e) {
            log.error('serialization error:', e.toString());
            return callback({error:e, code:500});
          }
        }

        transactions.push(data);
      });

      callback(null, transactions);
    }

  }



 /**
  *
  * getAccountTransactions
  * get transactions for a specific account
  * @param {Object} options
  * @param {Function} callback
  */
  self.getAccountTransactions = function (options, callback) {

    //prepare the sql query
    var query = prepareQuery();
    if (query.error) {
      callback(query);
      return;
    }

    log.debug(new Date().toISOString(), 'getting transactions:', options.account);

    //execute the query
    query.nodeify(function(err, resp) {

      if (err) {
        log.error(err.toString());
        callback({error:err, code:500});

      } else if (options.count) {
        log.debug(new Date().toISOString(), (resp[0].count || 0) + ' transaction(s) found');
        callback(null, parseInt(resp[0].count, 10));

      } else {
        log.debug(new Date().toISOString(), (resp ? resp.length : 0) + ' transaction(s) found');
        handleResponse(resp);
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

      var query = self.knex('account_transactions')
        .innerJoin('transactions', 'account_transactions.tx_hash', 'transactions.tx_hash')
        .where('account_transactions.account', options.account)

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

      if (options.count) {
        query.count();

      } else {
        query.select(self.knex.raw("encode(transactions.tx_raw, 'hex') as tx"))
          .select(self.knex.raw("encode(transactions.tx_meta, 'hex') as meta"))
          .select(self.knex.raw("encode(account_transactions.tx_hash, 'hex') as tx_hash"))
          .select('account_transactions.ledger_index')
          .select('account_transactions.tx_seq')
          .select('account_transactions.executed_time')
          .orderBy('account_transactions.ledger_index', descending ? 'desc' : 'asc')
          .orderBy('account_transactions.tx_seq', descending ? 'desc' : 'asc')
          .limit(options.limit || 20)

        if (options.offset) {
          query.offset(options.offset || 0);
        }
      }

      log.debug(query.toString());
      return query;
    }

   /**
    * handleResponse
    * @param {Object} rows
    * @param {Object} callback
    */
    function handleResponse (rows) {

      var transactions = [];

      rows.forEach(function(row) {
        var data = { };

        data.hash         = row.tx_hash.toUpperCase();
        data.ledger_index = parseInt(row.ledger_index, 10);
        data.date         = moment.unix(parseInt(row.executed_time, 10)).utc().format();

        if (options.binary) {
          data.tx   = row.tx;
          data.meta = row.meta;

        } else {
          try {
            data.tx   = binary.decode(row.tx.toUpperCase());
            data.meta = binary.decode(row.meta.toUpperCase());

          } catch (e) {
            log.error('serialization error:', e.toString());
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

      callback(null, transactions);
    }
  };

  self.getLastValidated = function (callback) {
    var query = self.knex('control')
      .select('value')
      .where('key', 'last_validated');

    //execute the query
    query.nodeify(function(err, resp) {
      var ledger;

      if (err) {
        log.error(err);
        return callback(err);
      }

      ledger = resp[0];

      callback(null, ledger ? JSON.parse(ledger.value) : null);
    });
  };
};

module.exports = DB;
