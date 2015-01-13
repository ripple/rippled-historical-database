var Promise    = require('bluebird');
var binformat  = require('ripple-lib').binformat;
var utils      = require('./utils');
var Hbase      = require('./modules/hbase-thrift');

var EPOCH_OFFSET = 946684800;
var LI_PAD       = 12;
var I_PAD        = 5;
var E_PAD        = 3;
var S_PAD        = 12;

var TX_TYPES   = { };
var TX_RESULTS = { };

Object.keys(binformat.tx).forEach(function(key) {
  TX_TYPES[key] = binformat.tx[key][0];
});

Object.keys(binformat.ter).forEach(function(key) {
  TX_RESULTS[key] = binformat.ter[key];
});

function HbaseClient() {
  Hbase.apply(this, arguments); 
};

HbaseClient.prototype = Object.create(Hbase.prototype);
HbaseClient.prototype.constructor = HbaseClient;

/**
 * saveLedger
 */

HbaseClient.prototype.saveLedger = function (ledger, callback) {
  var self       = this;
  var tableNames = [];
  var tables     = {
    ledgers             : { },
    lu_ledgers_by_index : { },
    lu_ledgers_by_time  : { }
  };
    
  var ledgerIndexKey = utils.padNumber(ledger.ledger_index, LI_PAD) + 
      '|' + ledger.ledger_hash;
  
  var ledgerTimeKey  = utils.formatTime(ledger.close_time) + 
      '|' + utils.padNumber(ledger.ledger_index, LI_PAD);
  
  //add formated ledger
  tables.ledgers[ledger.ledger_hash] = ledger;
  
  //add ledger index lookup
  tables.lu_ledgers_by_index[ledgerIndexKey] = {
    'f:ledger_index' : ledger.ledger_index,
    ledger_hash      : ledger.ledger_hash,
    parent_hash      : ledger.parent_hash,
    'f:close_time'   : ledger.close_time
  }
  
  //add ledger by time lookup
  tables.lu_ledgers_by_time[ledgerTimeKey] = {
    ledger_hash      : ledger.ledger_hash,
    parent_hash      : ledger.parent_hash,
    'f:ledger_index' : ledger.ledger_index,
    'f:close_time'   : ledger.close_time
  }
  
  tableNames = Object.keys(tables);
  
  Promise.map(tableNames, function(name) {
    return self.putRows(name, tables[name]);
  })
  .nodeify(function(err, resp) {   
    if (err) {
      self.log.error('error saving ledger:', ledger.ledger_index, err);
    } else {
      self.log.info('ledger saved:', ledger.ledger_index);
    }
    
    if (callback) {
      callback(err, resp);
    }
  });                
};

/**
 * saveTransaction
 */

HbaseClient.prototype.saveTransaction = function (tx, callback) {
  
  var self   = this;
  var data   = [ ];
  var type   = utils.padNumber(TX_TYPES[tx.TransactionType], E_PAD);
  var result = utils.padNumber(TX_RESULTS[tx.tx_result], E_PAD); 
  var ledgerIndex;
  var key;
  
  //transactions by time
  key = type + 
    '|' + result + 
    '|' + utils.formatTime(tx.executed_time) + 
    '|' + utils.padNumber(tx.ledger_index, LI_PAD) + 
    '|' + utils.padNumber(tx.tx_index, I_PAD);

  data.push({
    table   : 'lu_transactions_by_time',
    rowkey  : key,
    columns : {
      tx_hash           : tx.hash,
      'f:executed_time' : tx.executed_time,
      'f:ledger_index'  : tx.ledger_index,
      'f:type'          : tx.TransactionType,
      'f:result'        : tx.tx_result
    }
  });
  
  //transactions by account sequence
  key = tx.Account +
    '|' + type +
    '|' + result + 
    '|' + utils.padNumber(tx.Sequence, S_PAD);

  data.push({
    table   : 'lu_account_transactions',
    rowkey  : key,
    columns : {
      tx_hash           : tx.hash,
      'f:executed_time' : tx.executed_time,
      'f:ledger_index'  : tx.ledger_index,
      'f:type'          : tx.TransactionType,
      'f:result'        : tx.tx_result
    }
  });

  ledger_index = tx.ledger_index;
  
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
  data.push({
    table   : 'transactions',
    rowkey  : tx.hash,
    columns : tx
  });
  
  Promise.map(data, function(row) {
    return self.putRow(row.table, row.rowkey, row.columns);
  })
  .nodeify(function(err, resp) {  
    if (err) {
      self.log.error('error saving transaction:', ledger_index, tx.hash, err);
    } else {
      self.log.info('transaction saved:', ledger_index, tx.tx_index);
    }
    
    if (callback) {
      callback(err, resp);
    }
  });   
  
};

/**
 * SaveParsedData
 */

HbaseClient.prototype.saveParsedData = function (params, callback) {
  var self       = this;
  var tableNames = [];
  var tables     = {
    exchanges            : { },
    lu_account_exchanges : { },
    balance_changes      : { },
    lu_account_balance_changes : { },
    payments             : { },
    lu_account_payments  : { },
    accounts_created     : { },
    memos                : { },
    lu_account_memos     : { },
    lu_affected_account_transactions : { },
  };
  
  //add exchanges
  params.data.exchanges.forEach(function(ex) {
    var key = ex.base.currency + 
      '|' + (ex.base.issuer || '') + 
      '|' + ex.counter.currency + 
      '|' + (ex.counter.issuer || '') + 
      '|' + utils.formatTime(ex.time) + 
      '|' + utils.padNumber(ex.ledger_index, LI_PAD) + 
      '|' + utils.padNumber(ex.tx_index, I_PAD) +
      '|' + utils.padNumber(ex.node_index, I_PAD); //guarantee uniqueness
    
    var key2 = ex.buyer  + '|' + key;
    var key3 = ex.seller + '|' + key;
    var row  = {
      base_amount       : ex.base.amount,
      counter_amount    : ex.counter.amount,
      base_issuer       : ex.base.issuer,
      counter_issuer    : ex.counter.issuer || undefined,
      rate              : ex.rate,
      'f:buyer'         : ex.buyer,
      'f:seller'        : ex.seller,
      'f:taker'         : ex.taker,
      'f:tx_hash'       : ex.tx_hash,
      'f:executed_time' : ex.executed_time,
      'f:ledger_index'  : ex.ledger_index
    };
    
    tables.exchanges[key] = row;
    tables.lu_account_exchanges[key2] = row;
    tables.lu_account_exchanges[key3] = row;
  });
  
  //add balance changes
  params.data.balanceChanges.forEach(function(c) {
    var key = c.currency +
      '|' + (c.issuer ||  '') +
      '|' + utils.formatTime(c.time) + 
      '|' + utils.padNumber(c.ledger_index, LI_PAD) + 
      '|' + utils.padNumber(c.tx_index, I_PAD) +
      '|' + (c.node_index === 'fee' ? 'fee' : utils.padNumber(c.node_index, I_PAD));
    
    var row = {
      'f:currency'      : c.currency,
      'f:issuer'        : c.issuer,
      'f:account'       : c.account,
      change            : c.change,
      final_balance     : c.final_balance,
      'f:change_type'   : c.type,
      'f:tx_hash'       : c.tx_hash,
      'f:executed_time' : c.time,
      'f:ledger_index'  : c.ledger_index
    };
    
    tables.balance_changes[key] = row;
    
    key = c.account + '|' + key;
    tables.lu_account_balance_changes[key] = row;    
  });
  
  params.data.payments.forEach(function(p) {
    var key = p.currency +
      '|' + utils.formatTime(p.time) + 
      '|' + utils.padNumber(p.ledger_index, LI_PAD) + 
      '|' + utils.padNumber(p.tx_index, I_PAD);
    
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
      'f:tx_hash'       : p.tx_hash,
      'f:ledger_index'  : p.ledger_index
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
  params.data.accountsCreated.forEach(function(a) {
    var key = utils.formatTime(a.time) + 
      '|' + utils.padNumber(a.ledger_index, LI_PAD) + 
      '|' + utils.padNumber(a.tx_index, I_PAD);
    
    tables.accounts_created[key] = {
      'f:account'       : a.account,
      'f:parent'        : a.parent,
      balance           : a.balance,
      'f:tx_hash'       : a.tx_hash,
      'f:executed_time' : a.executed_time,
      'f:ledger_index'  : a.ledger_index
    };   
  });
  
  //add memos
  params.data.memos.forEach(function(m) {
    var key = utils.formatTime(m.time) + 
      '|' + utils.padNumber(m.ledger_index, LI_PAD) + 
      '|' + utils.padNumber(m.tx_index, I_PAD) + 
      '|' + utils.padNumber(m.memo_index, I_PAD);
    
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
      'f:executed_time'   : m.executed_time,
      'f:ledger_index'    : m.ledger_index
    };
    
    tables.lu_account_memos[m.account + '|' + key] = {
      rowkey            : key,
      'f:is_sender'     : true,
      'f:tag'           : m.source_tag,
      'f:tx_hash'       : m.tx_hash,
      'f:executed_time' : m.executed_time,
      'f:ledger_index'  : m.ledger_index
    }
    
    if (m.destination) {
      tables.lu_account_memos[m.destination + '|' + key] = {
        rowkey            : key,
        'f:is_sender'     : false,
        'f:tag'           : m.destination_tag,
        'f:tx_hash'       : m.tx_hash,
        'f:executed_time' : m.executed_time,
        'f:ledger_index'  : m.ledger_index
      }
    } 
  });
  
  //add affected accounts
  params.data.affectedAccounts.forEach(function(a) {
    var key = a.account + 
      '|' + utils.padNumber(TX_TYPES[a.tx_type], E_PAD) +
      '|' + utils.padNumber(TX_RESULTS[a.tx_result], E_PAD) +
      '|' + utils.formatTime(a.time) + 
      '|' + utils.padNumber(a.ledger_index, LI_PAD) + 
      '|' + utils.padNumber(a.tx_index, I_PAD);  
    
    tables.lu_affected_account_transactions[key] = {
      'f:type'          : a.tx_type,
      'f:result'        : a.tx_result,
      tx_hash           : a.tx_hash,
      'f:executed_time' : a.time,
      'f:ledger_index'  : a.ledger_index
    }
  });
  
  tableNames = Object.keys(tables);
  
  Promise.map(tableNames, function(name) {
    return self.putRows(name, tables[name]);
  })
  .nodeify(function(err, resp) {   
    if (err) {
      self.log.error('error saving parsed data:', params.ledgerIndex, params.txIndex);
    } else {
      self.log.info('parsed data saved:', params.ledgerIndex, params.txIndex);
    }
    
    if (callback) {
      callback(err, resp);
    }
  });
  
};

module.exports = HbaseClient;