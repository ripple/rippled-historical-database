var Promise    = require('bluebird');
var binformat  = require('ripple-lib').binformat;
var utils      = require('./utils');
var Hbase      = require('./modules/hbase-thrift');
var moment     = require('moment');
var SerializedObject = require('ripple-lib').SerializedObject;

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
 * getPayments
 * query payments
*/

HbaseClient.prototype.getPayments = function (options, callback) {
  var keyBase = options.account;
  var table   = 'lu_account_payments';
  var startRow;
  var endRow;

  if (options.start)
    startRow = keyBase + '|' + utils.formatTime(options.start);
  else startRow = keyBase + '|1';
  if (options.end)
    endRow   = keyBase + '|' + utils.formatTime(options.end);
  else endRow = keyBase + '|9';

  this.getScan({
    table    : table,
    startRow : startRow,
    stopRow  : endRow,
    limit    : options.limit
  }, function (err, rows) {
    callback(err, formatPayments(rows || []));
  });

  function formatPayments(rows) {
    rows.forEach(function(row, i) {
      var key = row.rowkey.split('|');

      rows[i].account          = key[0];
      rows[i].executed_time    = parseInt(row.executed_time, 10);
      rows[i].ledger_index     = parseInt(row.ledger_index, 10);
      rows[i].tx_index         = key[3];

      delete rows[i].rowkey;

      rows[i].destination_balance_changes = JSON.parse(row.destination_balance_changes);
      rows[i].source_balance_changes      = JSON.parse(row.source_balance_changes);
    });

    return rows;
  }
}

HbaseClient.prototype.getAccountBalanceChanges = function (options, callback) {
  var keyBase = options.account;
  var table = 'lu_account_balance_changes';
  var startRow;
  var endRow;

  if (!options.currency) callback({error:"must provide a currency", code:400});
  else {
    startRow = keyBase + '|';
    endRow = keyBase + '|';

    if (options.currency === "XRP") {
      startRow += 'XRP||';
      endRow = startRow + '9';
    }
    else if (options.currency) {
      startRow += options.currency + '|';
      if (options.issuer) {
        startRow += options.issuer + '|';
        endRow = startRow;
        if (options.start) startRow += utils.formatTime(options.start) + '|';
        if (options.end) endRow = startRow + utils.formatTime(options.end) + '|';
        else endRow += '9';
      } else endRow = startRow + 'z';
    }

    this.getScan({
      table    : table,
      startRow : startRow,
      stopRow  : endRow,
      limit    : options.limit
    }, function (err, rows) {
      callback(err, formatChanges(rows || []));
    });
  }

  function formatChanges(rows) {
    rows.forEach(function(row, i) {
      var key = row.rowkey.split('|');

      rows[i].tx_index       = parseInt(row.tx_index);
      rows[i].executed_time  = parseInt(row.executed_time, 10);
      rows[i].ledger_index   = parseInt(row.ledger_index, 10);
      rows[i].node_index     = parseInt(row.node_index, 10);

      delete rows[i].rowkey;
    });

    return rows;
  }
}

/**
 * getExchanges
 * query exchanges and
 * aggregated exchanges
 */

HbaseClient.prototype.getExchanges = function (options, callback) {
  var keyBase = options.base.currency + '|' + (options.base.issuer || '') +
      '|' + options.counter.currency + '|' + (options.counter.issuer || '');
  var startRow = keyBase + '|' + utils.formatTime(options.start);
  var endRow   = keyBase + '|' + utils.formatTime(options.end);
  var table    = 'exchanges';

  if      (options.interval === '1minute')  table = 'agg_exchange_1minute';
  else if (options.interval === '5minute')  table = 'agg_exchange_5minute';
  else if (options.interval === '15minute') table = 'agg_exchange_15minute';
  else if (options.interval === '30minute') table = 'agg_exchange_30minute';
  else if (options.interval === '1hour')    table = 'agg_exchange_1hour';
  else if (options.interval === '2hour')    table = 'agg_exchange_2hour';
  else if (options.interval === '4hour')    table = 'agg_exchange_4hour';
  else if (options.interval === '1day')     table = 'agg_exchange_1day';
  else if (options.interval === '3day')     table = 'agg_exchange_3day';
  else if (options.interval === '7day')     table = 'agg_exchange_7day';
  else if (options.interval === '1month')   table = 'agg_exchange_1month';
  else if (options.interval === '1year')    table = 'agg_exchange_1year';

  this.getScan({
    table      : table,
    startRow   : startRow,
    stopRow    : endRow,
    limit      : options.limit

  }, function (err, rows) {

    if (table === 'exchanges') {
      rows = formatExchanges(rows || []);
    } else {
      rows = formatAggregates(rows || []);
    }

    callback (err, rows);
  });

  /**
   * formatExchanges
   */

  function formatExchanges (rows) {
    rows.forEach(function(row, i) {
      var key = row.rowkey.split('|');
      rows[i].base = {
        amount   : parseFloat(row.base_amount),
        currency : key[0],
      };

      rows[i].counter = {
        amount   : parseFloat(row.counter_amount),
        currency : key[2],
      };

      if (row.base_issuer) {
        rows[i].base.issuer = row.base_issuer;
      }

      if (row.counter_issuer) {
        rows[i].counter.issuer = row.counter_issuer;
      }

      delete rows[i].base_amount;
      delete rows[i].counter_amount;
      delete rows[i].base_issuer;
      delete rows[i].counter_issuer;

      rows[i].rate             = parseFloat(row.rate);
      rows[i].ledger_index     = parseInt(row.ledger_index, 10);
      rows[i].tx_index         = parseInt(key[6], 10);
      rows[i].node_index       = parseInt(key[7], 10);
      rows[i].time             = utils.unformatTime(key[4]).unix();
    });

    return rows;
  }

  /**
   * formatAggregates
   */

  function formatAggregates (rows) {
    rows.forEach(function(row, i) {
      var key = row.rowkey.split('|');
      rows[i].base_volume    = parseFloat(row.base_volume),
      rows[i].counter_volume = parseFloat(row.counter_volume),
      rows[i].count          = parseInt(row.count, 10);
      rows[i].open           = parseFloat(row.open);
      rows[i].high           = parseFloat(row.high);
      rows[i].low            = parseFloat(row.low);
      rows[i].close          = parseFloat(row.close);
      rows[i].close_time     = parseInt(row.open_time, 10);
      rows[i].open_time      = parseInt(row.close_time, 10);
    });

    return rows;
  }
};

/**
 * getLedgersByIndex
 */

HbaseClient.prototype.getLedgersByIndex = function (options, callback) {
  var self  = this;
  var count = options.stopIndex - options.startIndex;

  this.getScan({
    table      : 'lu_ledgers_by_index',
    startRow   : utils.padNumber(options.startIndex, LI_PAD),
    stopRow    : utils.padNumber(options.stopIndex + 1, LI_PAD),
    descending : options.descending,
    limit      : options.limit

  }, function(err, resp) {

    if (resp && resp.length) {
      resp.forEach(function(row, i) {
        var rowkey = row.rowkey.split('|');
        resp[i].ledger_index = parseInt(rowkey[0], 10);
        resp[i].close_time = parseInt(resp[i].close_time, 10);
      });

      if (options.transactions) {
        Promise.map(resp, function(row) {
          self.getLedger({
            ledger_hash  : row.ledger_hash,
            transactions : true,

          }, function (err, ledger) {

          });

        }).nodeify(function(err, resp) {
          console.log(err, resp);
        });
        return;
      }
    }

    callback(err, resp);
  });
};

/**
 * getLedger
 */

HbaseClient.prototype.getLedger = function (options, callback) {
  var self = this;
  var ledger_hash = options.ledger_hash;

  //get by hash
  if (options.ledger_hash) {
    getLedgerByHash(options.ledger_hash);

  //get by index
  } else if (options.ledger_index) {
    self.getLedgersByIndex({
      startIndex : options.ledger_index,
      stopIndex : options.ledger_index
    }, function (err, resp) {

      if (err || !resp || !resp.length) {
        callback(err, null);
        return;
      }

      //use the ledger hash to get the ledger
      getLedgerByHash(resp[0].ledger_hash);
    });
  }


  function getLedgerByHash (hash) {
    var transactions = [];

    self.getRow('ledgers', hash, function(err, ledger) {

      if (err || !ledger) {
        callback(err, null);
        return;
      }

      ledger.ledger_index = parseInt(ledger.ledger_index, 10);
      ledger.close_time   = parseInt(ledger.close_time, 10);
      ledger.transactions = JSON.parse(ledger.transactions);

      //get transactions
      if (ledger.transactions.length && options.transactions) {
        transactions        = ledger.transactions;
        ledger.transactions = [];

        Promise.map(transactions, function (tx_hash) {
          return new Promise(function(resolve, reject) {
            self.getTransaction(tx_hash, function(err, tx) {
              if (err) reject(err);
              else     resolve(tx);
            });
          });
        }).nodeify(function(err, resp) {

          if (err) {
            callback(err, null);
            return;
          }

          //order by transaction index
          resp.sort(compare);
          ledger.transactions = resp;
          callback(err, ledger);
        });

      //return the ledger as is
      } else {
        callback(null, ledger);
      }
    });
  }

  function compare(a,b) {
    if (a.meta.TransactionIndex < b.meta.TransactionIndex)
       return -1;
    if (a.meta.TransactionIndex > b.meta.TransactionIndex )
      return 1;
    return 0;
  }
};

/**
 * getTransaction
 */

HbaseClient.prototype.getTransaction = function (tx_hash, callback) {
  var self = this;
  var transaction = { };

  self.getRow('transactions', tx_hash, function(err, tx) {

    if (err) {
      callback(err);
      return;
    }

    try {
      transaction.hash         = tx_hash;
      transaction.date         = moment.unix(tx.executed_time).utc().format();
      transaction.ledger_index = parseInt(tx.ledger_index, 10);

      transaction.tx   = new SerializedObject(tx.raw).to_json();
      transaction.meta = new SerializedObject(tx.meta).to_json();

    } catch (e) {
      callback(e);
      return;
    }

    callback(null, transaction);
  });
};

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

  var ledgerTimeKey  = utils.formatTime(ledger.close_time)
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
  this.saveTransactions([tx], callback);
};

/**
 * saveTransactions
 */

HbaseClient.prototype.saveTransactions = function (transactions, callback) {
  var self = this;
  var data = self.prepareTransactions(transactions);

  tableNames = Object.keys(data);

  Promise.map(tableNames, function(name) {
    return self.putRows(name, data[name]);
  })
  .nodeify(function(err, resp) {
    if (err) {
      self.log.error('error saving transaction(s)');
    } else {
      self.log.info(transactions.length + ' transaction(s) saved');
    }

    if (callback) {
      callback(err, resp);
    }
  });
};

/*
 * prepareTransactions
 */

HbaseClient.prototype.prepareTransactions = function (transactions) {
  var data = {
    transactions : { },
    lu_transactions_by_time : { },
    lu_account_transactions : { },
  };

  transactions.forEach(function(tx) {
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

    data.lu_transactions_by_time[key] = {
      tx_hash           : tx.hash,
      'f:executed_time' : tx.executed_time,
      'f:ledger_index'  : tx.ledger_index,
      'f:type'          : tx.TransactionType,
      'f:result'        : tx.tx_result
    }

    //transactions by account sequence
    key = tx.Account +
      '|' + type +
      '|' + result +
      '|' + utils.padNumber(tx.Sequence, S_PAD);

    data.lu_account_transactions[key] = {
      tx_hash           : tx.hash,
      'f:executed_time' : tx.executed_time,
      'f:ledger_index'  : tx.ledger_index,
      'f:type'          : tx.TransactionType,
      'f:result'        : tx.tx_result
    }

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
    data.transactions[tx.hash] = tx
  });

  return data;
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
    account_balance_changes : { },
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
      'f:base_currency'    : ex.base_currency,
      'f:base_issuer'      : ex.base.issuer || undefined,
      base_amount          : ex.base.amount,
      'f:counter_currency' : ex.counter.currency,
      'f:counter_issuer'   : ex.counter.issuer || undefined,
      counter_amount       : ex.counter.amount,
      rate                 : ex.rate,
      'f:buyer'            : ex.buyer,
      'f:seller'           : ex.seller,
      'f:taker'            : ex.taker,
      'f:tx_hash'          : ex.tx_hash,
      'f:executed_time'    : ex.executed_time,
      'f:ledger_index'     : ex.ledger_index,
      tx_index             : ex.tx_index,
      node_index           : ex.node_index
    };

    tables.exchanges[key] = row;
    tables.lu_account_exchanges[key2] = row;
    tables.lu_account_exchanges[key3] = row;
  });

  //add balance changes
  params.data.balanceChanges.forEach(function(c) {
    var suffix = '|' + utils.formatTime(c.time) +
      '|' + utils.padNumber(c.ledger_index, LI_PAD) +
      '|' + utils.padNumber(c.tx_index, I_PAD) +
      '|' + (c.node_index === 'fee' ? 'fee' : utils.padNumber(c.node_index, I_PAD));

    var key;

    var row = {
      'f:currency'      : c.currency,
      'f:issuer'        : c.issuer,
      'f:account'       : c.account,
      change            : c.change,
      final_balance     : c.final_balance,
      'f:change_type'   : c.type,
      'f:tx_hash'       : c.tx_hash,
      'f:executed_time' : c.time,
      'f:ledger_index'  : c.ledger_index,
      tx_index          : c.tx_index,
      node_index        : c.node_index
    };

    key = c.account + suffix;
    tables.account_balance_changes[c.account + suffix] = row;
    tables.account_balance_changes[c.issuer  + suffix] = row;
  });

  params.data.payments.forEach(function(p) {
    var key = utils.formatTime(p.time) +
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
      self.log.error('error saving parsed data');
    } else {
      self.log.info('parsed data saved');
    }

    if (callback) {
      callback(err, resp);
    }
  });

};

module.exports = HbaseClient;
