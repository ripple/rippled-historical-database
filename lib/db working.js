var Knex   = require('knex');
var Promise = require('bluebird');

var SerializedObject = require('ripple-lib').SerializedObject;

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
		idAttribute: null
	});

	var Account = bookshelf.Model.extend({
		tableName: 'accounts',
		idAttribute: null
	});

	var Account_Transaction = bookshelf.Model.extend({
		tableName: "account_transactions",
		idAttribute: null
	});

	//Parse ledger and add to database
	self.saveLedger = function (ledger, callback) {
		//console.log(ledger.transactions[0]);

		//Add all transactions to an array
		var toAdd = parse_transactions(ledger);

		//Add ledger info to array
		var ledger_info = parse_ledger(ledger);
		toAdd.push(ledger_info);

		//Add array atomically
		insert_all(toAdd);

		//Return all transactions in json
		//get_all();
	};

	//Given an array of queries, add to database atomically
	function insert_all(entry_array){
		bookshelf.transaction(function(t){
			console.log("Starting new DB call...");
			return Promise.map(entry_array, function(model){
				model.save({},{method: 'insert', transacting: t});
			});
		})
		.then(function(){
			console.log("Done.");
		});
	}

	//Pre-process all transactions
	function parse_transactions(ledger){
		var transaction_list = [];
		//Iterate through transactions, create Transaction and add it to array
		for (var i=0; i<ledger.transactions.length; i++){
			var transaction = ledger.transactions[i];
			var meta = transaction.metaData;
			delete transaction.metaData;
			var affected_nodes = meta.AffectedNodes;

			//Convert meta and tx (transaction minus meta) to hex
			var hex_tx = to_hex(transaction);
			var hex_meta = to_hex(meta);

			var tranaction_info = Transaction.forge({
				tx_hash: transaction.hash,
				tx_type: transaction.TransactionType,
				account: transaction.Account,
				tx_sequence: transaction.Sequence,
				ledger_index: ledger.seqNum,
				result: meta.TransactionResult,
				tx_raw: hex_tx,
				tx_meta: hex_meta
				//time
			});

			transaction_list.push(tranaction_info);

			//Iterate through affected nodes in each transaction,
			//create Account_Transaction and add it to array
			/*affected_nodes.forEach( function( affNode ) {
				var node = affNode.CreatedNode || affNode.ModifiedNode || affNode.DeletedNode;
				if (node.hasOwnProperty('FinalFields')){
					var ff = node.FinalFields;
					check_fields(ff);
				}
				if (node.hasOwnProperty('NewFields')){
					var nf = node.NewFields;
					check_fields(nf);
				}
			});*/

		}
		return transaction_list;
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

	function get_all(){
		new Transaction().fetchAll()
			.then(function(collection){
				console.log(collection.toJSON());
		});
	}

	function check_fields(fields){
		for (var key in fields){
			console.log(key, fields[key]);
			//Check if valid Ripple Address
		}
		if (fields.hasOwnProperty('HighLimit')){
			console.log(fields.HighLimit.issuer);
			//Check if valid Ripple Address
		}
		if (fields.hasOwnProperty('LowLimit')){
			console.log(fields.LowLimit.issuer);
			//Check if valid Ripple Address
		}
		if (fields.hasOwnProperty('TakerPays')){
			console.log(fields.TakerPays.issuer);
			//Check if valid Ripple Address
		}
		if (fields.hasOwnProperty('TakerGets')){
			console.log(fields.TakerGets.issuer);
			//Check if valid Ripple Address
		}
	}

};

module.exports = DB;