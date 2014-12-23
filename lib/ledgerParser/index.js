var log  = require('../log')('Ledger_Parser');
var SerializedObject = require('ripple-lib').SerializedObject;
var binformat = require('../../node_modules/ripple-lib/src/js/ripple/binformat');
var utils = require('../utils');

var EPOCH_OFFSET = 946684800;
var LI_PAD       = 12;
var I_PAD        = 5;
var E_PAD        = 3;
var S_PAD        = 12;
var Parser       = { };

var TX_TYPES   = { };
var TX_RESULTS = { };

Object.keys(binformat.tx).forEach(function(key) {
  TX_TYPES[key] = binformat.tx[key][0];
});

Object.keys(binformat.ter).forEach(function(key) {
  TX_RESULTS[key] = binformat.ter[key];
});

Parser.affectedAccounts = require('./affectedAccounts');
Parser.exchanges        = require('./exchanges');
Parser.balanceChanges   = require('./balanceChanges');
Parser.accountsCreated  = require('./accountsCreated');
Parser.memos            = require('./memos');
Parser.payment          = require('./payment');

Parser.parseHBase = function (ledger) {
  var tables = { 
    ledgers          : { },   
    transactions     : { },
    exchanges        : { },
    balance_changes  : { },
    payments         : { },
    accounts_created : { },
    memos            : { },
    lu_ledgers_by_index     : { },
    lu_ledgers_by_time      : { }, 
    lu_transactions_by_time : { },
    lu_account_transactions : { },
    lu_affected_account_transactions : { },
    lu_account_exchanges       : { },
    lu_account_balance_changes : { },
    lu_account_payments        : { },
    lu_account_memos           : { }
  };
  
  var data = this.parse(ledger);
  var ledgerIndexKey = pad(data.ledger.ledger_index, LI_PAD) + '|' + data.ledger.ledger_hash;
  var ledgerTimeKey  = utils.formatTime(ledger.close_time) + '|' + pad(data.ledger.ledger_index, LI_PAD);
  
  //add formated ledger
  tables.ledgers[ledger.ledger_hash] = data.ledger;
  
  //add ledger index lookup
  tables.lu_ledgers_by_index[ledgerIndexKey] = {
    ledger_hash : data.ledger.ledger_hash,
    close_time  : data.ledger.close_time
  }
  
  //add ledger by time lookup
  tables.lu_ledgers_by_time[ledgerTimeKey] = {
    ledger_hash  : data.ledger.ledger_hash,
    ledger_index : data.ledger.ledger_index
  }  
  
  //add transactions
  data.transactions.forEach(function(tx) {
    var type   = pad(TX_TYPES[tx.TransactionType], E_PAD);
    var result = pad(TX_RESULTS[tx.tx_result], E_PAD); 
    var key;

    //add transaction
    tables.transactions[tx.hash] = tx;
    
    //transactions by time
    key = type + 
      '|' + result + 
      '|' + utils.reverseTimestamp(tx.executed_time) + 
      '|' + pad(data.ledger.ledger_index, LI_PAD) + 
      '|' + pad(tx.tx_index, I_PAD);
    
    tables.lu_transactions_by_time[key] = {
      tx_hash : tx.hash
    }

    //transactions by account sequence
    key = tx.Account +
      '|' + type +
      '|' + result + 
      '|' + pad(tx.Sequence, S_PAD);
    
    tables.lu_account_transactions[key] = {
      tx_hash       : tx.hash,
      executed_time : tx.executed_time,
      ledger_index  : data.ledger.ledger_index
    }
  });
  
  data.exchanges.forEach(function(ex) {
    var key = ex.base.currency + 
      '|' + (ex.base.issuer || '') + 
      '|' + ex.counter.currency + 
      '|' + (ex.counter.issuer || '') + 
      '|' + utils.reverseTimestamp(ex.time) + 
      '|' + pad(ex.ledger_index, LI_PAD) + 
      '|' + pad(ex.tx_index, I_PAD) +
      '|' + pad(ex.node_index, I_PAD); //guarantee uniqueness
    
    var key2 = ex.account + '|' + key;
    var key3 = ex.counterparty + '|' + key;
    
    tables.exchanges[key] = {
        base_amount    : ex.base.amount,
        counter_amount : ex.counter.amount,
        rate           : ex.rate,
        account        : ex.account,
        counterparty   : ex.counterparty,
        tx_hash        : ex.tx_hash
    }
    
    tables.lu_account_exchanges[key2] = {
      base_amount    : ex.base.amount,
      counter_amount : ex.counter.amount,
      rate           : ex.rate,
      counterparty   : ex.counterparty,
      taker          : true,      
      tx_hash        : ex.tx_hash,
    };
    
    
    tables.lu_account_exchanges[key3] = {
      base_amount    : ex.base.amount,
      counter_amount : ex.counter.amount,
      rate           : ex.rate,
      counterparty   : ex.account,
      taker          : false,
      tx_hash        : ex.tx_hash
    };
    
  });
  
  data.balanceChanges.forEach(function(c) {
    var key = c.currency +
      '|' + (c.issuer ||  '') +
      '|' + utils.formatTime(c.time) + 
      '|' + pad(c.ledger_index, LI_PAD) + 
      '|' + pad(c.tx_index, I_PAD) +
      '|' + (c.node_index === 'fee' ? 'fee' : pad(c.node_index, I_PAD));
    
    tables.balance_changes[key] = {
      account : c.account,
      change  : c.change,
      final_balance : c.final_balance,
      type    : c.type,
      tx_hash : c.tx_hash
    };
    
    key = c.account + '|' + key;
    tables.lu_account_balance_changes[key] = {
      change  : c.change,
      final_balance : c.final_balance,
      type    : c.type,
      tx_hash : c.tx_hash
    };    
      
  });
  
  data.payments.forEach(function(p) {
    var key = p.currency +
      '|' + utils.formatTime(p.time) + 
      '|' + pad(p.ledger_index, LI_PAD) + 
      '|' + pad(p.tx_index, I_PAD);
    
    var payment = {
      source           : p.source,
      destination      : p.destination,
      amount           : p.amount,
      delivered_amount : p.delivered_amount,
      currency         : p.currency,
      source_currency  : p.source_currency,
      fee              : p.fee,
      source_balance_changes      : p.source_balance_changes,
      destination_balance_changes : p.destination_balance_changes,
      executed_time : p.time,  
      tx_hash       : p.tx_hash
    }
    
    if (p.max_amount) {
      payment.max_amount = p.max_amount;
    }
    
    if (p.destination_tag) {
      payment.destination_tag = p.destination_tag;
    }
    
    if (p.source_tag) {
      payment.source_tag = p.source_tag;
    }
    
    tables.payments[key] = payment;
    tables.lu_account_payments[p.source      + '|' + key] = payment;
    tables.lu_account_payments[p.destination + '|' + key] = payment;
  });
  
  data.accountsCreated.forEach(function(a) {
    var key = utils.formatTime(a.time) + 
      '|' + pad(a.ledger_index, LI_PAD) + 
      '|' + pad(a.tx_index, I_PAD);
    
    tables.accounts_created[key] = {
      account : a.account,
      parent  : a.parent,
      balance : a.balance,
      tx_hash : a.tx_hash
    };   
  });
  
  data.memos.forEach(function(m) {
    var key = utils.formatTime(m.time) + 
      '|' + pad(m.ledger_index, LI_PAD) + 
      '|' + pad(m.tx_index, I_PAD) + 
      '|' + pad(m.memo_index, I_PAD);
    
    delete m.time;
    delete m.ledger_index;
    delete m.tx_index;
    delete m.memo_index;
    
    tables.memos[m.account + '|' + key] = m;
    if (m.destination) {
      tables.memos[m.destination + '|' + key] = m;
    }
  });
  
  data.affectedAccounts.forEach(function(a) {
    var key = a.account + 
      '|' + pad(TX_TYPES[a.tx_type], E_PAD) +
      '|' + pad(TX_RESULTS[a.tx_result], E_PAD) +
      '|' + utils.reverseTimestamp(a.time) + 
      '|' + pad(a.ledger_index, LI_PAD) + 
      '|' + pad(a.tx_index, I_PAD);  
    
    tables.lu_affected_account_transactions[key] = {
      tx_hash : a.tx_hash,
    }
  });
  
  //console.log(tables.offers_exercised);
  //console.log(tables.offers_exercised_by_account);
  //console.log(tables.payments);
  //console.log(tables.payments_by_account);
  //console.log(tables.ledgers_by_index);
  //console.log(tables.ledgers_by_time);
  //console.log(tables.balance_changes);
  //console.log(tables.accounts_created);
  //console.log(tables.accounts_created_by_parent);
  //console.log(tables.memos);
  //console.log(tables);
  
  return tables;
};

Parser.parse = function(ledger) {
  var data = {
    ledger           : null,
    transactions     : [],
    affectedAccounts : [],
    accountsCreated  : [],
    exchanges        : [],
    balanceChanges   : [],
    payments         : [],
    memos            : [],
    valueMoved       : []
  }
  
  var transactions = ledger.transactions; 
  
  ledger.close_time   = ledger.close_time + EPOCH_OFFSET;
  ledger.transactions = [];
  
  transactions.forEach(function(transaction) {
    ledger.transactions.push(transaction.hash);
    var meta = transaction.metaData;
    var payment;
    
    delete transaction.metaData;
    
    try {
      transaction.raw  = toHex(transaction);
      transaction.meta = toHex(meta);
      
    } catch (e) {
      log.error(e, transaction.ledger_index, transaction.hash);
      return;
    }
    
    transaction.metaData        = meta;
    transaction.ledger_hash     = ledger.ledger_hash;
    transaction.ledger_index    = ledger.ledger_index;
    transaction.executed_time   = ledger.close_time;
    transaction.tx_index        = transaction.metaData.TransactionIndex;
    transaction.tx_result       = transaction.metaData.TransactionResult;
    
    data.transactions.push(transaction);
    
    data.exchanges        = data.exchanges.concat(Parser.exchanges(transaction));
    data.balanceChanges   = data.balanceChanges.concat(Parser.balanceChanges(transaction));
    data.accountsCreated  = data.accountsCreated.concat(Parser.accountsCreated(transaction));
    data.affectedAccounts = data.affectedAccounts.concat(Parser.affectedAccounts(transaction)); 
    data.memos            = data.memos.concat(Parser.memos(transaction)); 
    
    //parse payment
    payment = Parser.payment(transaction);
    if (payment) {
      data.payments.push(payment);
    }
  });
  
  data.ledger = ledger;
  
/* 
  console.log("ACCOUNTS CREATED");
  console.log(data.accountsCreated); 
  console.log("BALANCE CHANGES");
  console.log(data.balanceChanges);  
  console.log("OFFERS EXERCISED");
  console.log(data.offersExercised);
  console.log("AFFECTED ACCOUNTS");
  console.log(data.affectedAccounts);
  //console.log(data.transactions);
  console.log('done');
*/

  return data;
};

//Convert json to binary/hex to store as raw data
function toHex(input){
  return new SerializedObject.from_json(input).to_hex();
}


function pad(num, size) {
  var s = num+"";
  if (!size) size = 10;
  while (s.length < size) s = "0" + s;
  return s;
}

module.exports = Parser;