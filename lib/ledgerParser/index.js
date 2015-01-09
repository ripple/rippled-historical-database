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
    ledger_hash    : data.ledger.ledger_hash,
    parent_hash    : data.ledger.parent_hash,
    'f:close_time' : data.ledger.close_time
  }
  
  //add ledger by time lookup
  tables.lu_ledgers_by_time[ledgerTimeKey] = {
    ledger_hash    : data.ledger.ledger_hash,
    parent_hash    : data.ledger.parent_hash,
    ledger_index   : data.ledger.ledger_index,
    'f:close_time' : data.ledger.close_time
  }  
  
  //add transactions
  data.transactions.forEach(function(tx) {
    var type   = pad(TX_TYPES[tx.TransactionType], E_PAD);
    var result = pad(TX_RESULTS[tx.tx_result], E_PAD); 
    var key;

    //transactions by time
    key = type + 
      '|' + result + 
      '|' + utils.formatTime(tx.executed_time) + 
      '|' + pad(data.ledger.ledger_index, LI_PAD) + 
      '|' + pad(tx.tx_index, I_PAD);
    
    tables.lu_transactions_by_time[key] = {
      tx_hash           : tx.hash,
      'f:executed_time' : tx.executed_time,
    }

    //transactions by account sequence
    key = tx.Account +
      '|' + type +
      '|' + result + 
      '|' + pad(tx.Sequence, S_PAD);
    
    tables.lu_account_transactions[key] = {
      tx_hash           : tx.hash,
      'f:executed_time' : tx.executed_time,
      'f:ledger_index'  : data.ledger.ledger_index
    }
    
    tx['f:Account']         = tx.Account;
    tx['f:Sequence']        = tx.Sequence;
    tx['f:tx_result']       = tx.tx_result;
    tx['f:TransactionType'] = tx.TransactionType;
    tx['f:executed_time']   = tx.executed_time;
    tx['f:ledger_index']    = tx.ledger_index;
    tx['f:ledger_hash']     = tx.ledger_hash;

    delete tx.Account;
    delete tx.Sequence;
    delete tx.tx_result;
    delete tx.TransactionType;
    delete tx.executed_time;
    delete tx.ledger_index;
    delete tx.ledger_hash;
    
    //add transaction
    tables.transactions[tx.hash] = tx;
  });
  
  //add exchanges
  data.exchanges.forEach(function(ex) {
    var key = ex.base.currency + 
      '|' + (ex.base.issuer || '') + 
      '|' + ex.counter.currency + 
      '|' + (ex.counter.issuer || '') + 
      '|' + utils.formatTime(ex.time) + 
      '|' + pad(ex.ledger_index, LI_PAD) + 
      '|' + pad(ex.tx_index, I_PAD) +
      '|' + pad(ex.node_index, I_PAD); //guarantee uniqueness
    
    var key2 = ex.buyer  + '|' + key;
    var key3 = ex.seller + '|' + key;
    var row  = {
      base_amount       : ex.base.amount,
      counter_amount    : ex.counter.amount,
      rate              : ex.rate,
      'f:buyer'         : ex.buyer,
      'f:seller'        : ex.seller,
      'f:taker'         : ex.taker,
      'f:tx_hash'       : ex.tx_hash,
      'f:executed_time' : ex.executed_time
    };
    
    tables.exchanges[key] = row;
    tables.lu_account_exchanges[key2] = row;
    tables.lu_account_exchanges[key3] = row;
  });
  
  //add balance changes
  data.balanceChanges.forEach(function(c) {
    var key = c.currency +
      '|' + (c.issuer ||  '') +
      '|' + utils.formatTime(c.time) + 
      '|' + pad(c.ledger_index, LI_PAD) + 
      '|' + pad(c.tx_index, I_PAD) +
      '|' + (c.node_index === 'fee' ? 'fee' : pad(c.node_index, I_PAD));
    
    tables.balance_changes[key] = {
      'f:account'       : c.account,
      change            : c.change,
      final_balance     : c.final_balance,
      'f:change_type'   : c.type,
      'f:tx_hash'       : c.tx_hash,
      'f:executed_time' : c.time,
    };
    
    key = c.account + '|' + key;
    tables.lu_account_balance_changes[key] = {
      change            : c.change,
      final_balance     : c.final_balance,
      'f:change_type'   : c.type,
      'f:tx_hash'       : c.tx_hash,
      'f:executed_time' : c.time,
    };    
      
  });
  
  //add payments
  data.payments.forEach(function(p) {
    var key = p.currency +
      '|' + utils.formatTime(p.time) + 
      '|' + pad(p.ledger_index, LI_PAD) + 
      '|' + pad(p.tx_index, I_PAD);
    
    var payment = {
      'f:source'          : p.source,
      'f:destination'     : p.destination,
      amount              : p.amount,
      delivered_amount    : p.delivered_amount,
      'f:currency'        : p.currency,
      'f:source_currency' : p.source_currency,
      fee                 : p.fee,
      source_balance_changes      : p.source_balance_changes,
      destination_balance_changes : p.destination_balance_changes,
      'f:executed_time' : p.time,  
      'f:tx_hash'       : p.tx_hash
    }
    
    if (p.max_amount) {
      payment.max_amount = p.max_amount;
    }
    
    if (p.destination_tag) {
      payment['f:destination_tag'] = p.destination_tag;
    }
    
    if (p.source_tag) {
      payment['f:source_tag'] = p.source_tag;
    }
    
    tables.payments[key] = payment;
    tables.lu_account_payments[p.source      + '|' + key] = payment;
    tables.lu_account_payments[p.destination + '|' + key] = payment;
  });
  
  //add accounts created 
  data.accountsCreated.forEach(function(a) {
    var key = utils.formatTime(a.time) + 
      '|' + pad(a.ledger_index, LI_PAD) + 
      '|' + pad(a.tx_index, I_PAD);
    
    tables.accounts_created[key] = {
      'f:account'       : a.account,
      'f:parent'        : a.parent,
      balance           : a.balance,
      'f:tx_hash'       : a.tx_hash,
      'f:executed_time' : a.executed_time
    };   
  });
  
  //add memos
  data.memos.forEach(function(m) {
    var key = utils.formatTime(m.time) + 
      '|' + pad(m.ledger_index, LI_PAD) + 
      '|' + pad(m.tx_index, I_PAD) + 
      '|' + pad(m.memo_index, I_PAD);
    
    delete m.time;
    delete m.ledger_index;
    delete m.tx_index;
    delete m.memo_index;
    
    tables.memos[key] = {
      'f:account'         : m.account,
      'f:destination'     : m.destination,
      'f:source_tag'      : m.source_tag,
      'f:destination_tag' : m.destination_tag,
      memo_type           : m.memo_type,
      memo_data           : m.memo_data,
      memo_format         : m.memo_format,
      decoded_type        : m.decoded_type,
      decoded_data        : m.decoded_data,
      decoded_format      : m.decoded_format,
      type_encoding       : m.type_encoding,
      data_encoding       : m.data_encoding,
      format_encoding     : m.format_encoding,
      'f:tx_hash'         : m.tx_hash,
      'f:executed_time'   : m.executed_time
    };
    
    tables.lu_account_memos[m.account + '|' + key] = {
      rowkey            : key,
      'f:is_sender'     : true,
      'f:tag'           : m.source_tag,
      'f:tx_hash'       : m.tx_hash,
      'f:executed_time' : m.executed_time
    }
    
    if (m.destination) {
      tables.lu_account_memos[m.destination + '|' + key] = {
        rowkey            : key,
        'f:is_sender'     : false,
        'f:tag'           : m.destination_tag,
        'f:tx_hash'       : m.tx_hash,
        'f:executed_time' : m.executed_time
      }
    } 
  });
  
  //add affected accounts
  data.affectedAccounts.forEach(function(a) {
    var key = a.account + 
      '|' + pad(TX_TYPES[a.tx_type], E_PAD) +
      '|' + pad(TX_RESULTS[a.tx_result], E_PAD) +
      '|' + utils.formatTime(a.time) + 
      '|' + pad(a.ledger_index, LI_PAD) + 
      '|' + pad(a.tx_index, I_PAD);  
    
    tables.lu_affected_account_transactions[key] = {
      tx_hash           : a.tx_hash,
      'f:executed_time' : a.time,
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
      transaction.raw  = utils.toHex(transaction);
      transaction.meta = utils.toHex(meta);
      
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