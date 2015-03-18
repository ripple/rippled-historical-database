var config   = require('../../config/import.config');
var Logger   = require('../../lib/logger');
var Knex     = require('knex');
var Promise  = require('bluebird');
var moment   = require('moment');
var UInt160  = require('ripple-lib').UInt160;
var winston  = require('winston');
var SerializedObject = require('ripple-lib').SerializedObject;

var EPOCH_OFFSET = 946684800;
var hashErrorLog = new (require('winston').Logger)({
  transports: [
    new (winston.transports.Console)(),
    new (winston.transports.File)({ filename: './hashErrors.log' })
  ]
});

var log = new Logger({
  scope : 'postgres',
  level : config.get('logLevel') || 0,
  file  : config.get('logFile')
});

//Main
var DB = function(config) {
  var self = this;
  self.knex = Knex.initialize({
      client     : 'postgres',
      connection : config
  });
  var bookshelf = require('bookshelf')(self.knex);

  //Define Bookshelf models
  var Ledger = bookshelf.Model.extend({
      tableName: 'ledgers',
      idAttribute: 'ledger_hash'
  });

  var Transaction = bookshelf.Model.extend({
      tableName: 'transactions',
      idAttribute: 'tx_hash'
  });

//  var Account = bookshelf.Model.extend({
//    tableName: 'accounts',
//    idAttribute: 'account'
//  });

  var Account_Transaction = bookshelf.Model.extend({
      tableName: 'account_transactions',
      idAttribute: null
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

  //Parse ledger and add to database
  self.saveLedger = function (ledger, callback) {
    var ledger_info = [];
    var parsed_transactions = [];
    var tx_array = [];
    var acc_array = [];

    if (!callback) callback = function(){};

    try {
      //Preprocess ledger information
      ledger_info = parse_ledger(ledger);
      //Parse transactions
      parsed_transactions = parse_transactions(ledger);
      //Transactions in the format of {transactions, associated accounts}
      tx_array = parsed_transactions.tx;
      //List of all accounts
      acc_array = parsed_transactions.acc;

    } catch (e) {
      hashErrorLog.error(ledger.ledger_index, e.message);
      log.info("Unable to save ledger:", ledger.ledger_index);
      callback(null, ledger);
      return;
    }
/*
    //NOTE: not adding accounts table for now

    //Add all accounts encountered in ledger to database
    Promise.map(acc_array, function(account){
        //Check if account entry already exists
        return add_acc(account);
    }).catch(function(e){
      log.error(e);
    })

    //Atomically add ledger information, transactions, and account transactions
    .then(function(accounts) {
      var accountIDs = {};
      accounts.forEach(function(account) {
        accountIDs[account.get('account')] = account.get('account_id');
      });
*/

    bookshelf.transaction(function(t){
      log.info("Saving ledger:", ledger_info.get('ledger_index'));

      //Add ledger information
      return ledger_info.save({},{method: 'insert', transacting: t})
      .tap(function(l){

        //Go through transaction list
        return Promise.map(tx_array, function(model){

          //Add transaction to db
          return model.tx.save({},{method: 'insert', transacting: t})
          .then(function(tx){
            log.debug('New transaction:', model.hash);

            //Go through accounts associated with transaction
            return Promise.map(model.account, function(account){

              var fields = {
                tx_hash       : tx.get('tx_hash'),
                account       : account,
                ledger_index  : tx.get('ledger_index'),
                tx_seq        : tx.get('tx_seq'),
                executed_time : tx.get('executed_time'),
                tx_type       : tx.get('tx_type'),
                tx_result     : tx.get('tx_result'),
              };

              //Add account and tx_id to account transaction table
              return add_acctx(fields, t);
            });
          })
          .then(function(){
            log.debug("account transactions saved:", model.account.length);
          });
        });
      });
    })

    //Print error or done
    .nodeify(function(err, res){
      if (err){
        log.error('Error saving ledger:', err, ledger.ledger_index);
        callback(err);
      } else {
        log.debug('Done with ledger:', res.get('ledger_index'));
        callback(null, ledger);
      }
    });
  };

  //Pre-process all transactions
  function parse_transactions(ledger){
      var transaction_list = [];
      var all_addresses = [];
      //Iterate through transactions, create Transaction and add it to array
      for (var i=0; i<ledger.transactions.length; i++){
          var transaction = ledger.transactions[i];
          var meta = transaction.metaData;
          delete transaction.metaData;
          var affected_nodes = meta.AffectedNodes;

          //Convert meta and tx (transaction minus meta) to hex
          try {
           var hex_tx = to_hex(transaction);
           var hex_meta = to_hex(meta);

          } catch (e) {
            throw new Error (e + " " + transaction.hash);
            return null;
          }

          //Create transaction bookshelf model
          var tranaction_info = Transaction.forge({
            tx_hash       : self.knex.raw("decode('"+transaction.hash+"', 'hex')"),
            ledger_hash   : self.knex.raw("decode('"+ledger.ledger_hash+"', 'hex')"),
            ledger_index  : ledger.ledger_index,
            tx_type       : transaction.TransactionType,
            tx_seq        : meta.TransactionIndex,
            tx_result     : meta.TransactionResult,
            executed_time : ledger.close_time + EPOCH_OFFSET,
            account       : transaction.Account,
            account_seq   : transaction.Sequence,
            tx_raw        : self.knex.raw("decode('"+hex_tx+"', 'hex')"),
            tx_meta       : self.knex.raw("decode('"+hex_meta+"', 'hex')")
          });

          //Iterate through affected nodes in each transaction,
          //create Account_Transaction and add it to array
          var addresses = [];
          affected_nodes.forEach( function( affNode ) {
              var node = affNode.CreatedNode || affNode.ModifiedNode || affNode.DeletedNode;
              if (node.hasOwnProperty('FinalFields')){
                  var ff = node.FinalFields;
                  addresses = check_fields(ff, addresses);
              }
              if (node.hasOwnProperty('NewFields')){
                  var nf = node.NewFields;
                  addresses = check_fields(nf, addresses);
              }
          });

          //Check if account already in all_addresses list
          for (var j = 0; j<addresses.length; j++){
              address = addresses[j];
              if (check_unique(address, all_addresses)){
                  all_addresses.push(address);
              }
          }
          transaction_list.push({tx: tranaction_info, account: addresses, hash:transaction.hash});

      }
      //console.log(all_addresses);
      return {tx: transaction_list, acc: all_addresses};
  }

  //Pre-process ledger information, create Ledger, add to array
  function parse_ledger(ledger){
      var ledger_info = Ledger.forge({
          ledger_index: ledger.seqNum,
          ledger_hash: self.knex.raw("decode('"+ledger.ledger_hash+"', 'hex')"),
          parent_hash: self.knex.raw("decode('"+ledger.parent_hash+"', 'hex')"),
          total_coins: ledger.total_coins,
          closing_time: ledger.close_time + 946684800,
          close_time_res: ledger.close_time_resolution,
          accounts_hash: self.knex.raw("decode('"+ledger.account_hash+"', 'hex')"),
          transactions_hash: self.knex.raw("decode('"+ledger.transaction_hash+"', 'hex')"),
      });
      return ledger_info;
  }

  //Convert json to binary/hex to store as raw data
  function to_hex(input){
    hex = new SerializedObject.from_json(input).to_hex();
    return hex;
  }

  //Check all fields for ripple accounts to add to database
  function check_fields(fields, addresses){

      //CHANGE VALIDATION to:
      //Base.decode_check(Base.VER_ACCOUNT_ID, j);
      //From:
      /*var address = UInt160.from_json();
      console.log(address.is_valid())*/
      var candidate;
      //Iterate through all keys
      for (var key in fields){
          //Check if valid Ripple Address
          var address = UInt160.from_json(String(fields[key]));
          if(address.is_valid() && fields[key].charAt(0) == "r"){
              if (check_unique(fields[key], addresses)){
                  addresses.push(fields[key]);
              }
          }
      }
      //Check for four keywords
      if (fields.hasOwnProperty('HighLimit')){
          candidate = fields.HighLimit.issuer;
          check_ripple_id(candidate, addresses);
      }
      if (fields.hasOwnProperty('LowLimit')){
          candidate = fields.LowLimit.issuer;
          check_ripple_id(candidate, addresses);
      }
      if (fields.hasOwnProperty('TakerPays')){
          candidate = fields.TakerPays.issuer;
          check_ripple_id(candidate, addresses);
      }
      if (fields.hasOwnProperty('TakerGets')){
          candidate = fields.TakerGets.issuer;
          check_ripple_id(candidate, addresses);
      }

      return addresses;
  }

  //Check if ripple id is valid
  function check_ripple_id(candidate, addresses){
      var address = UInt160.from_json(String(candidate));
          if(address.is_valid() && candidate.charAt(0) == "r"){
              if (check_unique(candidate, addresses)){
                  addresses.push(candidate);
              }
      }
  }

  //Checks whether a token is in an array
  function check_unique(entry, array){
      is_unique = true;
      for (index in array){
          if (array[index] == entry){
              is_unique = false;
              break;
          }
      }
      return is_unique;
  }

  //Find account_id and add with tx_id to account_transactions table
  function add_acctx(fields, t){
    return Account_Transaction.forge(fields)
    .save({},{method: 'insert', transacting: t}).then(function(result){
      log.debug('Added account transaction:', result.get('account'));
    });
  }

  function add_acc(account){
    return new Account({account: account})
    .fetch()
    .then(function(model) {
      if (model === null) {

        //Add
        return Account.forge({account: account}).save().then(function(model){
          log.info(account, 'Added.');
          return model;
        }).catch(function(e){

          if (e.code !== '23505') {
            log.error("unable to save account:", e);
            return null;
          }

          //account was concurrently added
          log.info(account, "exists.");
          return new Account({account: account})
          .fetch()
          .then(function(model){
            return model;
          });
        });

      } else {
        log.info(account, "exists.");
        return model;
      }
    });
  }

  /**
  * getLedgers
  * get a specific group of ledgers from the db
  */

  self.getLedgers = function (options, callback) {

    var query = self.knex('ledgers')
      .where('ledger_index', '>=', options.startIndex)
      .where('ledger_index', '<=', options.stopIndex)
      .select('ledger_index')
      .select(self.knex.raw("encode(ledger_hash, 'hex') as ledger_hash"))
      .select(self.knex.raw("encode(parent_hash, 'hex') as parent_hash"))
      .orderBy('ledger_index', 'desc');

    //execute the query
    query.nodeify(function(err, ledgers) {
      if (err) {
        log.error(err);
        return callback(err);
      }

      callback(null, ledgers);
    });

  };

  self.getLatestLedger = function(callback) {
     var query = self.knex('ledgers')
      .select('ledger_index')
      .select(self.knex.raw("encode(ledger_hash, 'hex') as ledger_hash"))
      .select(self.knex.raw("encode(parent_hash, 'hex') as parent_hash"))
      .limit(1)
      .orderBy('ledger_index', 'desc');

    //execute the query
    query.nodeify(function(err, ledgers) {

      if (err) {
        log.error(err);
        return callback(err);
      }

      callback(null, ledgers[0]);
    });
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

  self.setLastValidated = function (lastValidated, callback) {
    query = self.knex('control')
      .where('key', 'last_validated')
      .update({value:JSON.stringify(lastValidated)});

    //execute the query
    query.nodeify(function(err, resp) {
      if (err) {
        callback(err);
        return;

      //last valid doesnt exist,
      //so create it
      } else if (!resp) {
        query = self.knex('control')
          .insert({
            key:'last_validated',
            value: JSON.stringify(lastValidated)});

        query.nodeify(function(err, resp){
          callback(err, resp ? (resp.rowCount || 0) : null);
        });

      } else {
        callback(null, resp);
        return;
      }
    });
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
              row.tx   = new SerializedObject(row.tx).to_json();
              row.meta = new SerializedObject(row.meta).to_json();
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
};


module.exports = DB;
