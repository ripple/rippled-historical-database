var Knex    = require('knex');
var Promise = require('bluebird');

var SerializedObject = require('ripple-lib').SerializedObject;
var UInt160 = require('ripple-lib').UInt160;

//Main
var DB = function(config) {
	var self = this;
	var knex = Knex.initialize({
		client     : config.dbtype,
		connection : config.db
	});
	var bookshelf = require('bookshelf')(knex);
	
	//Define Bookshelf models
	var Ledger = bookshelf.Model.extend({
		tableName: 'ledgers',
		idAttribute: null
	});

	var Transaction = bookshelf.Model.extend({
		tableName: 'transactions',
		idAttribute: 'tx_id'
	});

	var Account = bookshelf.Model.extend({
		tableName: 'accounts',
		idAttribute: null
	});

	var Account_Transaction = bookshelf.Model.extend({
		tableName: 'account_transactions',
		idAttribute: null
	});

	//Parse ledger and add to database
	self.saveLedger = function (ledger, callback) {
		var ledger_info = [];
		var parsed_transactions = [];
		var tx_array = [];
		var acc_array = [];

		//Preprocess ledger information
		ledger_info = parse_ledger(ledger);
		//Parse transactions
		parsed_transactions = parse_transactions(ledger);
		//Transactions in the format of {transactions, associated accounts}
		tx_array = parsed_transactions.tx;
		//List of all accounts
		acc_array = parsed_transactions.acc;

		//Add all accounts encountered in ledger to database
		Promise.map(acc_array, function(account){
			//Check if account entry already exists
			return new Account({account: account})
				.fetch()
				.then(function(model){
					if (model == null){
						console.log(account, 'does not exist.');
						//Add
						return Account.forge({account: account}).save().then(function(){
							console.log('Added.');
						});
					}
					else{
						console.log(account, "exists.");
					}
				})
		})
		//Atomically add ledger information, transactions, and account transactions
		.then(function(){
			bookshelf.transaction(function(t){
				console.log("Starting new DB call...");
				//Add ledger information
				return ledger_info.save({},{method: 'insert', transacting: t})
					.tap(function(l){
						li = l.get('ledger_index');
						//Go through transaction list
						return Promise.map(tx_array, function(model){
							//Add transaction to db
							return model.tx.save({ledger_index: li},{method: 'insert', transacting: t})
								//Get newly added tx_id
								.then(function(tx){
									tx_id = tx.get('tx_id');
									console.log('New transaction:', tx_id);
									//Go through accounts associated with transaction
									return Promise.map(model.account, function(account){
										//Get account id
										return new Account({account: account})
											.fetch()
											.then(function(model){
												//Add account transaction
												console.log('Account id:', model.get('account_id'), 'tx_id:', tx_id);
												return Account_Transaction.forge({account_id: model.get('account_id'), tx_id: tx_id})
													.save({},{method: 'insert', transacting: t}).then(function(){
														console.log('Added account transaction.');
													});
											})
									})
								})
						})
					})
			})
			//Print error or done
			.nodeify(function(err, res){
				if (err){console.log(err)}
				else {console.log('Done with ledger.')}
			})
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
			//Why?
			if (meta === undefined){
				console.log('No meta?');
				break;
			}
			delete transaction.metaData;
			var affected_nodes = meta.AffectedNodes;

			//Convert meta and tx (transaction minus meta) to hex
			var hex_tx = to_hex(transaction);
			var hex_meta = to_hex(meta);

			//Create transaction bookshelf model
			var tranaction_info = Transaction.forge({
				tx_hash: transaction.hash,
				tx_type: transaction.TransactionType,
				account: transaction.Account,
				tx_sequence: transaction.Sequence,
				result: meta.TransactionResult,
				tx_raw: hex_tx,
				tx_meta: hex_meta
				//time
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
			
			for (var i = 0; i<addresses.length; i++){
				address = addresses[i]
				if (checkUnique(address, all_addresses)){
					all_addresses.push(address);
				}
			}
			transaction_list.push({tx: tranaction_info, account: addresses});

		}
		//console.log(all_addresses);
		return {tx: transaction_list, acc: all_addresses};
	}

	//Pre-process ledger information, create Ledger, add to array
	function parse_ledger(ledger){
		var ledger_info = Ledger.forge({
			ledger_index: ledger.seqNum,
			ledger_hash: ledger.hash,
			parent_hash: ledger.parent_hash,
			total_coins: ledger.total_coins,
			close_time: ledger.close_time,
			close_time_resolution: ledger.close_time_resolution,
			close_time_human: ledger.close_time_human,
			accounts_hash: ledger.account_hash,
			transactions_hash: ledger.transaction_hash
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
		/*var address = UInt160.from_json();
		console.log(address.is_valid())*/

		//Iterate through all keys
		for (var key in fields){
			//Check if valid Ripple Address
			var address = UInt160.from_json(String(fields[key]));
			if(address.is_valid() && fields[key].charAt(0) == "r"){
				if (checkUnique(fields[key], addresses)){
					addresses.push(fields[key]);
				}
			}
		}
		//Check for four keywords
		if (fields.hasOwnProperty('HighLimit')){
			var candidate = fields.HighLimit.issuer;
			check_ripple_id(candidate, addresses);
		}
		if (fields.hasOwnProperty('LowLimit')){
			var candidate = fields.LowLimit.issuer;
			check_ripple_id(candidate, addresses);
		}
		if (fields.hasOwnProperty('TakerPays')){
			var candidate = fields.TakerPays.issuer;
			check_ripple_id(candidate, addresses);
		}
		if (fields.hasOwnProperty('TakerGets')){
			var candidate = fields.TakerGets.issuer;
			check_ripple_id(candidate, addresses);
		}

		return addresses;
	}


	function check_ripple_id(candidate, addresses){
		var address = UInt160.from_json(String(candidate));
			if(address.is_valid() && candidate.charAt(0) == "r"){
				if (checkUnique(candidate, addresses)){
					addresses.push(candidate);
				}
		}
	}

	//Checks whether a token is in an array
	function checkUnique(entry, array){
		is_unique = true;
		for (index in array){
			if (array[index] == entry){
				is_unique = false;
				break
			}
		}
		return is_unique
	}

};



module.exports = DB;