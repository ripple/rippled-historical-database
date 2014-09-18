var config = require('./config.json');
var Knex   = require('knex');
var knex   = Knex.initialize({
	client: 'postgres',
	connection : config.db
});
var bookshelf = require('bookshelf')(knex);
var Promise = require('bluebird');
var db = { };

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

db.saveLedger = function (ledger, callback) {
	
	//use knex.transactions  
	//console.log(ledger);
	console.log(ledger.transactions);

	//Add all transactions to an array
	//transactions_list = parse_transactions(ledger);

	//Add ledger info to database
	//ledger_info = parse_ledger(ledger);

	//console.log(transactions_list.length);
	//insert_all(transactions_list);


};

function insert_all(entry_array){
	bookshelf.transaction(function(t){
		console.log("Starting new DB call...");
		return Promise.map(entry_array, function(model){
			model.save({},{method: 'insert', transacting: t});
		})
	})
	.then(function(){
		console.log("Done.");
	});
}

function parse_transactions(ledger){
	var transaction_list = [];
	for (var i=0; i<ledger.transactions.length; i++){
		var entry = ledger.transactions[i];
		var tranaction_info = Transaction.forge({
			hash: entry.hash,
			type: entry.TransactionType,
			account: entry.Account,
			sequence: entry.Sequence,
			//ledger_index:,
			//result:,
			//raw:,
			//meta:,
		});
		transaction_list.push(tranaction_info);
	}
	return transaction_list;
}

function parse_ledger(ledger){
	var ledger_info = Ledger.forge({
		hash: ledger.hash,
		index: ledger.seqNum,
		parent_hash: ledger.parent_hash,
		total_coins: ledger.total_coins,
		close_time: ledger.close_time,
		close_time_resolution: ledger.close_time_resolution,
		close_time_human: ledger.close_time_human,
		account_hash: ledger.account_hash,
		transaction_hash: ledger.transaction_hash
	});
	return ledger_info;
}


module.exports = db;