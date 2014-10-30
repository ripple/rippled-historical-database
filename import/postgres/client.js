var Knex    = require('knex');
var Promise = require('bluebird');
var log     = require('../../lib/log')('postgres');
var moment  = require('moment');

var SerializedObject = require('ripple-lib').SerializedObject;
var UInt160 = require('ripple-lib').UInt160;

var winston = require('winston');
var hashErrorLog = new (require('winston').Logger)({
  transports: [
    new (winston.transports.Console)(),
    new (winston.transports.File)({ filename: './hashErrors.log' })
  ]   
});

var EPOCH_OFFSET = 946684800;

log.level(3);

//Main
var DB = function(config) {
	var self = this;
	self.knex = Knex.initialize({
		client     : config.dbtype,
		connection : config.db
	});
	var bookshelf = require('bookshelf')(self.knex);
	
	//Define Bookshelf models
	var Ledger = bookshelf.Model.extend({
		tableName: 'ledgers',
		idAttribute: 'ledger'
	});

	var Transaction = bookshelf.Model.extend({
		tableName: 'transactions',
		idAttribute: 'tx_hash'
	});

//	var Account = bookshelf.Model.extend({
//		tableName: 'accounts',
//		idAttribute: 'account'
//	});

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
              log.info('New transaction:', model.hash);

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
              log.info("account transactions saved:", model.account.length);
            });
          });
        });
      })

      //Print error or done
      .nodeify(function(err, res){
        if (err){
          log.info('Error saving ledger:', err, ledger.ledger_index);	
          callback(err);
        } else {
          log.info('Done with ledger:', res.get('ledger_index'));
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
			ledger: self.knex.raw("decode('"+ledger.ledger_hash+"', 'hex')"),
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
        log.debug('Added account transaction:', result.get('account_id'), result.get('tx_id'));
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
      
    }
    
    return this;
};


module.exports = DB;