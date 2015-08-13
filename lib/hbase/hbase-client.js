var Promise    = require('bluebird');
var ripple     = require('ripple-lib');
var moment     = require('moment');
var smoment    = require('../smoment');
var utils      = require('../utils');
var Hbase      = require('./hbase-thrift');
var Parser     = require('../ledgerParser');

var SerializedObject = ripple._DEPRECATED.SerializedObject;

var EPOCH_OFFSET = 946684800;
var LI_PAD       = 12;
var I_PAD        = 5;
var E_PAD        = 3;
var S_PAD        = 12;

var exchangeIntervals = [
  '1minute',
  '5minute',
  '15minute',
  '30minute',
  '1hour',
  '2hour',
  '4hour',
  '1day',
  '3day',
  '7day',
  '1month',
  '1year'
];

function HbaseClient() {
  Hbase.apply(this, arguments);
}

HbaseClient.prototype = Object.create(Hbase.prototype);
HbaseClient.prototype.constructor = HbaseClient;

/**
 * getLastValidated
 */

HbaseClient.prototype.getLastValidated = function(callback) {
  this.getRow({
    table: 'control',
    rowkey: 'last_validated'
  }, callback);
};

/**
 * getStats
 */

HbaseClient.prototype.getStats = function(options, callback) {

  var interval = options.interval || 'day';
  var startRow = interval + '|' + options.start.hbaseFormatStartRow();
  var endRow = interval + '|' + options.end.hbaseFormatStopRow();
  var includeFamilies = options.metrics || options.family ? false : true;
  var filterString;

  if (options.family) {
    filterString = 'FamilyFilter (=, \'binary:' + options.family + '\')';
  }

  this.getScanWithMarker(this, {
    table: 'agg_stats',
    startRow: startRow,
    stopRow: endRow,
    limit: options.limit || Infinity,
    descending: options.descending,
    marker: options.marker,
    filterString: filterString,
    columns: options.metrics,
    includeFamilies : includeFamilies
  }, function(err, res) {

    if (res) {
      res.interval = interval;

      results = {
        rows: [ ],
        marker: res.marker,
        interval: interval
      };

      // group by family
      if (includeFamilies) {
        res.rows.forEach(function(row, i) {
          var parts = row.rowkey.split('|');
          var stats = {
            date: utils.unformatTime(parts[1]).format(),
            type: { },
            result: { },
            metric: { }
          };

          for (var key in row) {
            if (key === 'rowkey') {
              continue;
            }

            parts = key.split(':');
            stats[parts[0]][parts[1]] = Number(row[key]);
          }

          res.rows[i] = stats;
        });

      } else {
        res.rows.forEach(function(row, i) {
          var parts = row.rowkey.split('|');
          delete row.rowkey;

          for (var key in row) {
            row[key] = Number(row[key]);
          }

          row.date = utils.unformatTime(parts[1]).format();
        });
      }
    }
    callback(err, res);
  });
}

/**
 * getStatsRow
 */

HbaseClient.prototype.getStatsRow = function(options) {

  if (!options) {
    options = { };
  }

  var self = this;
  var time = options.time || moment.utc();
  var interval = options.interval || 'day';
  var rowkey;

  time.startOf(interval === 'week' ? 'isoWeek' : interval);
  rowkey = interval + '|' + utils.formatTime(time);

  return new Promise(function(resolve, reject) {
    self._getConnection(function(err, connection) {

      if (err) {
        reject(err);
        return;
      }

      connection.client.getRow(self._prefix + 'agg_stats',
                               rowkey,
                               null,
                               function(err, rows) {
        var parts;
        var stats = {
          time: time.format(),
          interval: interval,
          type: { },
          result: { },
          metric: {
            accounts_created: 0,
            transaction_count: 0,
            ledger_count: 0,
            tx_per_ledger: 0.0,
            ledger_interval: 0.0
          }
        };

        if (err) {
          reject(err);

        } else if (!rows.length) {
          resolve(stats);

        } else {
          for (var key in rows[0].columns) {
            parts = key.split(':');
            stats[parts[0]][parts[1]] = Number(rows[0].columns[key].value);
          }

          resolve(stats);
        }
      });
    });
  });
};

/**
 * getPayments
 */

HbaseClient.prototype.getPayments = function(options, callback) {
  var filters = [];
  var filterString;
  var table;
  var startRow;
  var endRow;

  if (options.interval) {
    table = 'agg_payments';
    startRow = options.interval +
      '|' + options.currency +
      '|' + (options.issuer || '') +
      '|' + options.start.hbaseFormatStartRow();
    endRow = options.interval +
      '|' + options.currency +
      '|' + (options.issuer || '') +
      '|' + options.end.hbaseFormatStopRow()

  } else {
    table = 'payments';
    startRow = options.start.hbaseFormatStartRow();
    endRow = options.end.hbaseFormatStopRow();

    if (options.currency) {
      filters.push({
        qualifier: 'currency',
        value: options.currency,
        family: 'f', comparator: '='
      });
    }

    if (options.issuer) {
      filters.push({
        qualifier: 'issuer',
        value: options.issuer,
        family: 'f', comparator: '='
      });
    }

    if (options.reduce) {
      options.columns = [
        'd:delivered_amount',
        'f:currency',
        'f:issuer'
      ];
    }
  }

  filterString = this.buildSingleColumnValueFilters(filters);

  this.getScanWithMarker(this, {
    table: table,
    startRow: startRow,
    stopRow: endRow,
    limit: options.limit || Infinity,
    descending: options.descending,
    marker: options.marker,
    filterString: filterString,
    columns: options.columns
  }, function(err, res) {
    var amount;

    if (options.interval) {
      if (res && res.rows) {
        res.rows.forEach(function(row) {
          row.count = Number(row.count);
          row.amount = Number(row.amount);
          row.average = Number(row.average);
        });
      }

    } else if (options.reduce) {
      amount = 0;

      if (res && res.rows) {
        res.rows.forEach(function(row) {
          amount += Number(row.delivered_amount);
        });

        res = {
          amount: amount,
          count: res.rows.length
        };

      } else {
        res = {
          amount: 0,
          count: 0
        };
      }

    } else {
      res.rows = formatPayments(res.rows || []);
    }
    callback(err, res);
  });

  function formatPayments(rows) {
    rows.forEach(function(row) {

      row.executed_time = parseInt(row.executed_time, 10);
      row.ledger_index = parseInt(row.ledger_index, 10);
      row.tx_index = parseInt(row.tx_index, 10);

      if (row.destination_balance_changes) {
        row.destination_balance_changes = JSON.parse(row.destination_balance_changes);
      }
      if (row.source_balance_changes) {
        row.source_balance_changes = JSON.parse(row.source_balance_changes);
      }
    });

    return rows;
  }
};

/**
 * getAggregateAccountPayments
 */

HbaseClient.prototype.getAggregateAccountPayments = function(options) {
  var self = this;
  var keys = [ ];
  var start;
  var end;

  if (options.account) {

    if (options.date) {
      keys.push(options.date.hbaseFormatStartRow() + '|' + options.account);

    } else {
      start = options.start.moment;
      end = options.end.moment;

      while(end.diff(start)>=0) {
        keys.push(utils.formatTime(start) + '|' + options.account);
        start.add(1, 'day');
      }
    }

    return new Promise (function(resolve, reject) {
      self.getRows({
        table: 'agg_account_payments',
        rowkeys: keys
      }, function(err, rows) {
        if (err) {
          reject(err);
          return;
        }

        resolve(formatRows(rows || [], keys));
      });
    });

  } else {
    return new Promise (function(resolve, reject) {
      console.log(options.start.hbaseFormatStartRow(), options.end.hbaseFormatStopRow());
      self.getScan({
        table: 'agg_account_payments',
        startRow: options.start.hbaseFormatStartRow(),
        stopRow: options.end.hbaseFormatStopRow(),
      }, function (err, rows) {
        if (err) {
          reject(err);
          return;
        }

        resolve(formatRows(rows || []));
      });
    });
  }

  function formatRows(rows, keys) {
    var results = { };

    var Bucket  = function(key) {
      this.receiving_counterparties = [],
      this.sending_counterparties   = [],
      this.payments = [],
      this.payments_sent        = 0,
      this.payments_received    = 0,
      this.high_value_sent      = 0,
      this.high_value_received  = 0,
      this.total_value_sent     = 0,
      this.total_value_received = 0,
      this.total_value          = 0

      if (key) {
        var parts    = key.split('|');
        this.date    = utils.unformatTime(parts[0]).format();
        this.account = parts[1];
      }

      return this;
    }

    rows.forEach(function(row) {
      var key = row.rowkey;
      var parts;

      row.sending_counterparties   = JSON.parse(row.sending_counterparties || '[]');
      row.receiving_counterparties = JSON.parse(row.receiving_counterparties || '[]');
      row.payments                 = JSON.parse(row.payments || '[]');
      row.payments_sent        = Number(row.payments_sent || 0);
      row.payments_received    = Number(row.payments_received || 0);
      row.high_value_sent      = Number(row.high_value_sent || 0);
      row.high_value_received  = Number(row.high_value_received || 0);
      row.total_value_sent     = Number(row.total_value_sent || 0);
      row.total_value_received = Number(row.total_value_received || 0);
      row.total_value          = Number(row.total_value || 0);
      delete row.rowkey;

      //keys will be present on a
      //single account lookup, in
      //which we will add empty
      //buckets as needed.
      if (keys) {
        results[key] = row;
      }

      if (!row.account) {
        parts = key.split('|');
        row.date    = utils.unformatTime(parts[0]).format();
        row.account = parts[1];
      }
    });

    if (keys) {
      rows = [ ];
      keys.forEach(function(key) {
        rows.push(results[key] || new Bucket(key));
      });
    }

    return rows;
  }
}

/**
 * getAccountPayments
 * query account payments
 */

HbaseClient.prototype.getAccountPayments = function (options, callback) {
  var table    = 'account_payments';
  var startRow = options.account + '|' + options.start.hbaseFormatStartRow();
  var endRow   = options.account + '|' + options.end.hbaseFormatStopRow();
  var type;

  if(options.currency) {
    options.currency= options.currency.toUpperCase();
  }

  if(options.type) {
    if(options.type === 'sent') {
      type = 'source'
    } else if(options.type === 'received') {
      type = 'destination'
    }
  }

  var maybeFilters =
  [{ qualifier: 'currency', value: options.currency, family: 'f', comparator: '=' },
   { qualifier: type, value: options.account, family: 'f', comparator: '=' }];

  var filterString= this.buildSingleColumnValueFilters(maybeFilters);

  this.getScanWithMarker(this, {
    table        : table,
    startRow     : startRow,
    stopRow      : endRow,
    limit        : options.limit,
    descending   : options.descending,
    marker       : options.marker,
    filterString : filterString,

  }, function (err, res) {
    if (res) {
      res.rows = formatPayments(res.rows || []);
    }

    callback(err, res);
  });

  function formatPayments(rows) {
    rows.forEach(function(row, i) {
      var key = row.rowkey.split('|');

      row.executed_time    = parseInt(row.executed_time, 10);
      row.ledger_index     = parseInt(row.ledger_index, 10);
      row.tx_index         = key[3];

      if (row.destination_balance_changes) {
        row.destination_balance_changes = JSON.parse(row.destination_balance_changes);
      }
      if (row.source_balance_changes) {
        row.source_balance_changes = JSON.parse(row.source_balance_changes);
      }
    });

    return rows;
  }
}

/**
 * getAccountTransaction
 */

HbaseClient.prototype.getAccountTransaction = function(options, callback) {
  var self = this;

  self.getRow({
    table: 'lu_account_transactions',
    rowkey: options.account + '|' + utils.padNumber(options.sequence, S_PAD)
  }, function(err, resp) {
    if (err) {
      callback(err);
    } else if (resp) {
      self.getTransaction({
        tx_hash: resp.tx_hash,
        binary: options.binary
      }, callback);

    } else {
      callback(err, resp);
    }
  });
};

/**
 * getAccountTransactions
 */

HbaseClient.prototype.getAccountTransactions = function(options, callback) {
  var self = this;
  var hashes = [];
  var filters = [];
  var table;
  var startRow;
  var stopRow;

  if (options.minSequence || options.maxSequence) {
    table = 'lu_account_transactions';
    startRow = options.account + '|' + utils.padNumber(options.minSequence || 0, S_PAD);
    stopRow = options.account + '|' + utils.padNumber(options.maxSequence || 999999999999999, S_PAD);

  } else {
    table = 'lu_affected_account_transactions';
    startRow = options.account + '|' + options.start.hbaseFormatStartRow();
    stopRow = options.account + '|' + options.end.hbaseFormatStopRow();
  }

  if (options.type) {
    filters.push({
      qualifier: 'type',
      value: options.type,
      family: 'f',
      comparator: '='
    });
  }

  if (options.result) {
    filters.push({
      qualifier: 'result',
      value: options.result,
      family: 'f',
      comparator: '='
    });
  }

  self.getScanWithMarker(self, {
    table: table,
    startRow: startRow,
    stopRow: stopRow,
    descending: options.descending,
    limit: options.limit,
    filterString: self.buildSingleColumnValueFilters(filters)

  }, function(err, resp) {

    if (err) {
      callback(err);

    } else if (!resp.rows.length) {
      callback(null, []);

    } else {
      resp.rows.forEach(function(tx) {
        hashes.push(tx.tx_hash);
      });

      self.getTransactions({
        hashes: hashes,
        binary: options.binary,
        marker: resp.marker
      }, callback);
    }

  });

  //callback('unavailable');
};

/**
 * getAccountBalanceChanges
 */

HbaseClient.prototype.getAccountBalanceChanges = function(options, callback) {
  var table = 'account_balance_changes';
  var startRow = options.account + '|' + options.start.hbaseFormatStartRow();
  var endRow = options.account + '|' + options.end.hbaseFormatStopRow();

  if(options.currency) {
    options.currency= options.currency.toUpperCase();
  }

  var maybeFilters =
  [{ qualifier: 'currency', value: options.currency, family: 'f', comparator: '=' },
   { qualifier: 'issuer', value:options.issuer, family: 'f', comparator: '=' }]

  var filterString= this.buildSingleColumnValueFilters(maybeFilters);

  this.getScanWithMarker(this, {
    table        : table,
    startRow     : startRow,
    stopRow      : endRow,
    limit        : options.limit,
    marker       : options.marker,
    descending   : options.descending,
    filterString : filterString
  }, function (err, res) {
    res.rows= formatChanges(res.rows || []);
    callback(err, res);
  });

  function formatChanges(rows) {
    rows.forEach(function(row, i) {
      var key = row.rowkey.split('|');

      rows[i].tx_index       = parseInt(row.tx_index);
      rows[i].executed_time  = parseInt(row.executed_time, 10);
      rows[i].ledger_index   = parseInt(row.ledger_index, 10);
      rows[i].node_index     = parseInt(row.node_index, 10);
    });

    return rows;
  }
}

/**
 * getExchangeRate
 */

HbaseClient.prototype.getExchangeRate = function(options) {
  var self = this;
  if (!options.base) {
    options.base = {currency:'XRP'};
  } if (!options.counter) {
    options.counter = {currency:'XRP'};
  }

  //default to strict mode
  options.strict = options.strict === false ? false : true;

  if (options.base.currency === options.counter.currency &&
      options.base.issuer === options.counter.issuer) {
    return Promise.resolve(1);

  } else {
    return Promise.all([
      getDailyRate(),
      getLatestRate()
    ])
    .then(function(rates) {

      if (rates[0] && rates[1]) {
        return Promise.resolve((rates[0] + rates[1])/2);
      } else {
        return Promise.resolve(rates[1]);
      }
    });
  }

  // get daily vwap rate
  function getDailyRate() {
    return new Promise(function(resolve, reject) {
      var start = smoment(options.date.format());
      start.moment.startOf('day');
      self.getExchanges({
        base: options.base,
        counter: options.counter,
        interval: '1day',
        start: start,
        end: options.date,
        descending: false,
        limit: 1
      }, function(err, resp) {
        if (err) {
          reject(err);
        } else {
          resolve(resp.rows[0] ? Number(resp.rows[0].vwap || 0) : 0);
        }
      });
    });
  }

  // get last 50 trades within 2 weeks
  function getLatestRate() {
    return new Promise(function(resolve, reject) {
      var start = smoment(options.date.format());
      start.moment.subtract(14, 'days');
      self.getExchanges({
        base: options.base,
        counter: options.counter,
        start: start,
        end: options.date,
        descending: true,
        limit: 50,
        reduce: true
      }, function(err, resp) {
        if (err) {
          reject(err);
        } else if (resp.reduced) {
          if (resp.reduced.count >= 10 || !options.strict) {
            resolve(Number(resp.reduced.vwap || 0));
          } else {
            resolve(0);
          }
        }
      });
    });
  }
};

/**
 * getExchanges
 * query exchanges and
 * aggregated exchanges
 */

HbaseClient.prototype.getExchanges = function (options, callback) {
  var base = options.base.currency + '|' + (options.base.issuer || '');
  var counter = options.counter.currency + '|' + (options.counter.issuer || '');
  var table;
  var keyBase;
  var startRow;
  var endRow;
  var descending;
  var columns;

  if (counter.toLowerCase() > base.toLowerCase()) {
    keyBase = base + '|' + counter;

  } else {
    keyBase = counter + '|' + base;
    options.invert = true;
  }

  if (!options.interval) {
    table      = 'exchanges';
    descending = options.descending ? true : false;
    options.unreduced = true;

    //only need certain columns
    if (options.reduce) {
      columns = [
        'd:base_amount',
        'd:counter_amount',
        'd:rate',
        'f:executed_time'
      ];
    }

  } else if (exchangeIntervals.indexOf(options.interval) !== -1) {
    keyBase    = options.interval + '|' + keyBase;
    descending = options.descending ? true : false;
    table      = 'agg_exchanges';

  } else {
    callback('invalid interval: ' + options.interval);
    return;
  }

  startRow = keyBase + '|' + options.start.hbaseFormatStartRow();
  endRow   = keyBase + '|' + options.end.hbaseFormatStopRow();

  if (options.autobridged) {
    options.filterstring = "DependentColumnFilter('f', 'autobridged_currency')";
    if (columns) {
      columns.push('f:autobridged_currency');
    }
  }

  this.getScanWithMarker(this, {
    table: table,
    startRow: startRow,
    stopRow: endRow,
    marker: options.marker,
    limit: options.limit,
    descending: descending,
    columns: columns,
    filterString: options.filterstring
  }, function (err, resp) {

    if (!resp) {
      resp = {rows: []};
    }

    if (!resp.rows) {
      resp.rows = [];
    }

    if (options.reduce && options.unreduced) {
      if (descending) {
        resp.rows.reverse();
      }

      resp.reduced = reduce(resp.rows);
    } else if (table === 'exchanges') {
      resp.rows = formatExchanges(resp.rows);
    } else {
      resp.rows = formatAggregates(resp.rows);
    }

    callback(err, resp);
  });

  /**
   * formatExchanges
   */

  function formatExchanges (rows) {
    rows.forEach(function(row) {
      var key = row.rowkey.split('|');

      delete row.base_issuer;
      delete row.base_currency;
      delete row.counter_issuer;
      delete row.counter_currency;

      row.base_amount    = parseFloat(row.base_amount);
      row.counter_amount = parseFloat(row.counter_amount);
      row.rate           = parseFloat(row.rate);
      row.offer_sequence = Number(row.offer_sequence || 0);
      row.ledger_index   = Number(row.ledger_index);
      row.tx_index       = Number(key[6]);
      row.node_index     = Number(key[7]);
      row.time           = utils.unformatTime(key[4]).unix();
    });

    if (options.invert) {
      rows = invertPair(rows);
    }

    return rows;
  }

  /**
   * formatAggregates
   */

  function formatAggregates (rows) {
    rows.forEach(function(row) {
      var key = row.rowkey.split('|');
      row.base_volume    = parseFloat(row.base_volume),
      row.counter_volume = parseFloat(row.counter_volume),
      row.count          = parseInt(row.count, 10);
      row.open           = parseFloat(row.open);
      row.high           = parseFloat(row.high);
      row.low            = parseFloat(row.low);
      row.close          = parseFloat(row.close);
      row.close_time     = parseInt(row.close_time, 10);
      row.open_time      = parseInt(row.open_time, 10);
    });

    if (options.invert) {
      rows = invertPair(rows);
    }

    return rows;
  }

  /**
  * if the base/counter key was inverted, we need to swap
  * some of the values in the results
  */

  function invertPair (rows) {
    var swap;
    var i;

    if (options.unreduced) {

      for (i=0; i<rows.length; i++) {
        rows[i].rate = 1/rows[i].rate;

        //swap base and counter vol
        swap = rows[i].base_amount;
        rows[i].base_amount    = rows[i].counter_amount;
        rows[i].counter_amount = swap;

        //swap buyer and seller
        swap = rows[i].buyer;
        rows[i].buyer  = rows[i].seller;
        rows[i].seller = swap;
      }

    } else {

      for (i=0; i<rows.length; i++) {

        //swap base and counter vol
        swap = rows[i].base_volume;
        rows[i].base_volume    = rows[i].counter_volume;
        rows[i].counter_volume = swap;

        //swap high and low
        swap = 1/rows[i].high;
        rows[i].high = 1/rows[i].low;
        rows[i].low  = swap;

        //invert open, close, vwap
        rows[i].open  = 1/rows[i].open;
        rows[i].close = 1/rows[i].close;
        rows[i].vwap  = 1/rows[i].vwap;
      }
    }

    return rows;
  }

  /**
   * reduce
   * reduce all rows
   */

  function reduce (rows) {

   var reduced = {
      open  : 0,
      high  : 0,
      low   : Infinity,
      close : 0,
      base_volume    : 0,
      counter_volume : 0,
      count      : 0,
      open_time  : 0,
      close_time : 0
    };

    rows = formatExchanges(rows);

    if (rows.length) {
      reduced.open_time  = moment.unix(rows[0].time).utc().format();
      reduced.close_time = moment.unix(rows[rows.length-1].time).utc().format();

      reduced.open  = rows[0].rate;
      reduced.close = rows[rows.length -1].rate;
      reduced.count = rows.length;

    } else {
      reduced.low = 0;
      return reduced;
    }

    rows.forEach(function(row) {
      if (options.base.currency    === 'XRP' && row.base_amount < 0.0001)    return;
      if (options.counter.currency === 'XRP' && row.counter_amount < 0.0001) return;

      reduced.base_volume    += row.base_amount;
      reduced.counter_volume += row.counter_amount;

      if (row.rate < reduced.low)  reduced.low  = row.rate;
      if (row.rate > reduced.high) reduced.high = row.rate;
    });

    reduced.vwap = reduced.counter_volume / reduced.base_volume;
    return reduced;
  }
};

HbaseClient.prototype.getAccountExchanges = function (options, callback) {

  var maybeFilters =
  [{ qualifier: 'base_currency', value: options.base.currency, family: 'f', comparator: '=' },
   { qualifier: 'base_issuer' , value: options.base.issuer, family: 'f', comparator: '=' },
   { qualifier: 'counter_currency', value: options.counter.currency, family: 'f', comparator: '=' },
   { qualifier: 'counter_issuer' , value: options.counter.issuer, family: 'f', comparator: '=' }];

  var filterString= this.buildSingleColumnValueFilters(maybeFilters);

  this.getScanWithMarker(this, {
    table          : 'account_exchanges',
    startRow       : options.account + '|' + options.start.hbaseFormatStartRow(),
    stopRow        : options.account + '|' + options.end.hbaseFormatStopRow(),
    descending     : options.descending,
    limit          : options.limit,
    filterString   : filterString,
    marker         : options.marker

  }, function(err, res) {

      if(!res) res= {};

      if (!res.rows) res.rows = [];

      res.rows.forEach(function(row) {

        row.base_amount    = parseFloat(row.base_amount);
        row.counter_amount = parseFloat(row.counter_amount);
        row.rate           = parseFloat(row.rate);
        row.ledger_index   = parseInt(row.ledger_index, 10);
        row.tx_index       = parseInt(row.tx_index || 0);
        row.node_index     = parseInt(row.node_index || 0);
      });

      callback(err, res);
  });
};

/**
 * getLedgersByIndex
 */

HbaseClient.prototype.getLedgersByIndex = function (options, callback) {
  var self  = this;

  this.getScan({
    table      : 'lu_ledgers_by_index',
    startRow   : utils.padNumber(Number(options.startIndex), LI_PAD),
    stopRow    : utils.padNumber(Number(options.stopIndex) + 1, LI_PAD),
    descending : options.descending,
    limit      : options.limit

  }, function(err, resp) {

    if (resp && resp.length) {
      resp.forEach(function(row, i) {
        var rowkey = row.rowkey.split('|');
        resp[i].ledger_index = parseInt(rowkey[0], 10);
        resp[i].close_time = parseInt(resp[i].close_time, 10);
      });
    }

    callback(err, resp);
  });
};

/**
 * getLedgersByTime
 */

HbaseClient.prototype.getLedgersByTime = function (options, callback) {
  var self  = this;

  this.getScan({
    table      : 'lu_ledgers_by_time',
    startRow   : smoment(options.start).hbaseFormatStartRow(),
    stopRow    : smoment(options.end).hbaseFormatStopRow(),
    descending : options.descending,
    limit      : options.limit

  }, callback);
};

/**
 * getLedger
 */

HbaseClient.prototype.getLedger = function (options, callback) {
  var self = this;
  var ledger_hash = options.ledger_hash;

  //get by hash
  if (options.ledger_hash) {
    getLedgerByHash(options);

  //get ledger by close time
  } else if (options.closeTime) {
    self.getLedgersByTime({
      start      : moment.utc(0),
      end        : options.closeTime,
      descending : true,
      limit      : 1
    }, function (err, resp){
      if (err || !resp || !resp.length) {
        callback(err, null);
        return;
      }

      //use the ledger hash to get the ledger
      options.ledger_hash = resp[0].ledger_hash;
      getLedgerByHash(options);
    });

  //get by index, or get latest
  } else {
    self.getLedgersByIndex({
      startIndex : options.ledger_index || 0,
      stopIndex  : options.ledger_index || 999999999999,
      descending : true,
      limit      : 2

    }, function (err, resp) {

      if (err || !resp || !resp.length) {
        callback(err, null);
        return;

      //submit error on duplicate ledger index
      } else if (resp.length > 1 && options.ledger_index) {
        console.log(resp);
        callback('duplicate ledger index: ' + options.ledger_index, null);
        return;
      }

      //use the ledger hash to get the ledger
      options.ledger_hash = resp[0].ledger_hash;
      getLedgerByHash(options);
    });
  }


  function getLedgerByHash(options) {
    var hashes = [];

    self.getRow({
      table: 'ledgers',
      rowkey: options.ledger_hash
    }, function(err, ledger) {

      if (err || !ledger) {
        callback(err, null);
        return;
      }

      ledger.ledger_index = parseInt(ledger.ledger_index, 10);
      ledger.close_time = parseInt(ledger.close_time, 10);
      ledger.transactions = JSON.parse(ledger.transactions);

      // get transactions
      if (ledger.transactions.length &&
          (options.expand || options.binary)) {
        hashes = ledger.transactions;
        ledger.transactions = [];
        self.getTransactions({
          hashes: hashes,
          binary: options.binary,
          include_ledger_hash: options.include_ledger_hash

        }, function(err, resp) {

          if (err) {
            callback(err, null);
            return;

          } else if (hashes.length !== resp.rows.length && !options.invalid) {
            callback('missing transaction: ' +
                   resp.rows.length + ' of ' +
                   hashes.length + ' found');
            return;
          }

          ledger.transactions = resp.rows;
          callback(err, ledger);
        });

      // return the ledger as is
      } else if (options.transactions) {
        callback(null, ledger);

      // remove tranactions array
      } else {
        delete ledger.transactions;
        callback(null, ledger);
      }
    });
  }
};

/**
 * getTransaction
 */

HbaseClient.prototype.getTransaction = function(options, callback) {
  options.hashes = [options.tx_hash];

  this.getTransactions(options, function(err, resp) {
    if (resp) {
      resp = resp.rows ? resp.rows[0] : undefined;
    }

    callback(err, resp);
  });
};

/**
 * getTransactions
 */

HbaseClient.prototype.getTransactions = function(options, callback) {
  var self = this;

  if (options.hashes) {
    getTransactionsFromHashes(options, callback);

  } else {
    getTransactionsByTime(options, function(err, resp) {
      if (resp && resp.rows) {
        options.marker = resp.marker; // replace/add marker
        options.hashes = resp.rows;
        getTransactionsFromHashes(options, callback);

      } else if (err) {
        callback(err);

      } else {
        callback(null, {rows: []});
      }
    });
  }

  function getTransactionsByTime(opts, cb) {
    var filters = [];

    if (opts.type) {
      filters.push({
        qualifier: 'type',
        value: opts.type,
        family: 'f',
        comparator: '='
      });
    }

    if (opts.result) {
      filters.push({
        qualifier: 'result',
        value: opts.result,
        family: 'f',
        comparator: '='
      });
    }

    self.getScanWithMarker(self, {
      table: 'lu_transactions_by_time',
      startRow: opts.start.hbaseFormatStartRow(),
      stopRow: opts.end.hbaseFormatStopRow(),
      marker: opts.marker,
      descending: opts.descending,
      limit: opts.limit,
      filterString: self.buildSingleColumnValueFilters(filters),
      columns: ['d:tx_hash','f:type','f:result']
    }, function(err, resp) {

      if (resp) {
        resp.rows.forEach(function(row, i) {
          resp.rows[i] = row.tx_hash;
        });
      }

      cb(err, resp);
    });
  }

  function getTransactionsFromHashes(opts, cb) {
    var results = {
      marker: opts.marker,
      rows: []
    };

    self.getRows({
      table: 'transactions',
      rowkeys: opts.hashes,
      columns: [
        'f:executed_time',
        'f:ledger_index',
        'f:ledger_hash',
        'd:raw',
        'd:meta',
        'd:tx_index',
      ]
    }, function(err, resp) {

      if (err) {
        cb(err);
        return;
      }

      if (resp) {

        if (opts.ledger) {
          resp.sort(compare);
        }

        resp.forEach(function(row, i) {
          var tx = { };

          try {
            tx.hash = row.rowkey;
            tx.date = moment.unix(row.executed_time).utc().format();
            tx.ledger_index = Number(row.ledger_index);
            if (opts.include_ledger_hash) {
              tx.ledger_hash = row.ledger_hash;
            }

            if (opts.binary) {
              tx.tx = row.raw;
              tx.meta = row.meta;

            } else {
              tx.tx = new SerializedObject(row.raw).to_json();
              tx.meta = new SerializedObject(row.meta).to_json();
            }

            results.rows.push(tx);

          } catch (e) {
            cb(e);
            return;
          }
        });
      }

      cb(null, results);
    });
  }

  function compare(a, b) {
    if (Number(a.tx_index) < Number(b.tx_index)) {
      return -1;
    } else {
      return 1;
    }
  }
};

/**
 * getAccounts
 */

HbaseClient.prototype.getAccounts = function(options, callback) {
  var self = this;
  var params;

  /**
   * formatRows
   */

  function formatRows(resp) {
    var rows = [ ];
    var parts;

    for (var i = 0; i < resp.length; i++) {

      // aggregate rows
      if (options.interval) {
        parts = resp[i].rowkey.split('|');
        rows.push({
          date: utils.unformatTime(parts[1]).format(),
          count: Number(resp[i].accounts_created)
        });

      //genesis ledger accounts
      } else if (resp[i].genesis_balance) {
        delete resp[i].rowkey;
        rows.push({
          account: resp[i].account,
          executed_time: moment.unix(resp[i].executed_time),
          ledger_index: Number(resp[i].ledger_index),
          genesis_balance: resp[i].genesis_balance,
          genesis_index: Number(resp[i].genesis_index)
        });

      // single account rows
      } else {

        delete resp[i].rowkey;
        delete resp[i].tx_index;
        delete resp[i].client;

        resp[i].ledger_index = Number(resp[i].ledger_index);
        resp[i].executed_time = moment.unix(resp[i].executed_time)
          .utc()
          .format();

        rows.push(resp[i]);
      }
    }

    return rows;
  }

  /**
   * getReducedAccounts
   */

  function getReducedAccounts() {
    var paramsList = [];
    var filterString = 'FirstKeyOnlyFilter() AND KeyOnlyFilter()';
    var start;
    var end;

    start = moment.utc(options.start.format());
    end = moment.utc(options.end.format());

    if (Math.abs(end.diff(start, 'days')) > 31 && !options.parent) {

      // individual up to the first full week
      paramsList.push({
        table: 'accounts_created',
        startRow: utils.formatTime(start),
        stopRow: utils.formatTime(start.startOf('isoWeek').add(1, 'week')),
        descending: false,
        filterString: filterString
      });

      // individual from the last week to end
      paramsList.push({
        table: 'accounts_created',
        stopRow: utils.formatTime(end),
        startRow: utils.formatTime(end.startOf('isoWeek')),
        descending: false,
        filterString: filterString
      });

      // aggregate for the rest
      paramsList.push({
        table: 'agg_stats',
        startRow: 'week|' + utils.formatTime(start),
        stopRow: 'week|' + utils.formatTime(end),
        columns: ['metric:accounts_created'],
        descending: false
      });

    } else {
      if (options.parent) {
        filterString = self.buildSingleColumnValueFilters([{
          qualifier: 'parent',
          family: 'f',
          comparator: '=',
          value: options.parent
        }]);
      }

      paramsList.push({
        table: 'accounts_created',
        startRow: utils.formatTime(start),
        stopRow: utils.formatTime(end),
        descending: false,
        filterString: filterString
      });
    }

    Promise.map(paramsList, function(p) {
      return new Promise(function(resolve, reject) {
        self.getScan(p, function(err, resp) {
          var count = 0;
          if (err) {
            reject(err);

          } else if (p.table === 'accounts_created') {
            resolve(resp.length);

          } else {
            for (var i = 0; i < resp.length; i++) {
              count += Number(resp[i].accounts_created);
            }

            resolve(count);
          }
        });
      });
    }).nodeify(function(err, resp) {
      var total = 0;
      if (resp) {
        resp.forEach(function(count) {
          total += count;
        });

        resp = {rows: [total]};
      }

      callback(err, resp);
    });
  }

  // reduced to count
  if (options.reduce) {
    getReducedAccounts(options);
    return;

  // counts over time
  } else if (options.interval) {
    params = {
      table: 'agg_stats',
      startRow: options.interval + '|' + options.start.hbaseFormatStartRow(),
      stopRow: options.interval + '|' + options.end.hbaseFormatStopRow(),
      columns: ['metric:accounts_created']
    };

  // individual rows
  } else {
    params = {
      table: 'accounts_created',
    };

    if (options.parent) {
      params.filterString = self.buildSingleColumnValueFilters([{
        qualifier: 'parent',
        family: 'f',
        comparator: '=',
        value: options.parent
      }]);
    }

    if (options.account) {
      params.filterString = self.buildSingleColumnValueFilters([{
        qualifier: 'account',
        family: 'f',
        comparator: '=',
        value: options.account
      }]);

      options.start = smoment(0);
      options.end = smoment();
    }

    params.startRow = options.start.hbaseFormatStartRow(),
    params.stopRow = options.end.hbaseFormatStopRow()
  }

  params.limit = options.limit;
  params.descending = options.descending;
  params.marker = options.marker;

  self.getScanWithMarker(this, params, function(err, resp) {
    if (resp && resp.rows) {
      resp.rows = formatRows(resp.rows);
    }

    callback(err, resp);
  });
};

/**
 * saveLedger
 */

HbaseClient.prototype.saveLedger = function (ledger, callback) {
  var self       = this;
  var tableNames = [];
  var tables     = self.prepareLedgerTables(ledger);

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
      self.log.error('error saving transaction(s)', err);
    } else {
      self.log.info(transactions.length + ' transaction(s) saved');
    }

    if (callback) {
      callback(err, transactions.length);
    }
  });
};

/**
 * prepareLedgerTables
 */

HbaseClient.prototype.prepareLedgerTables = function (ledger) {
  var tables = {
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
    ledger_hash      : ledger.ledger_hash,
    parent_hash      : ledger.parent_hash,
    'f:ledger_index' : ledger.ledger_index,
    'f:close_time'   : ledger.close_time
  }

  //add ledger by time lookup
  tables.lu_ledgers_by_time[ledgerTimeKey] = {
    ledger_hash      : ledger.ledger_hash,
    parent_hash      : ledger.parent_hash,
    'f:ledger_index' : ledger.ledger_index,
    'f:close_time'   : ledger.close_time
  }

  return tables;
}

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
    var ledgerIndex;
    var key;

    //transactions by time
    key = utils.formatTime(tx.executed_time) +
      '|' + utils.padNumber(tx.ledger_index, LI_PAD) +
      '|' + utils.padNumber(tx.tx_index, I_PAD);

    data.lu_transactions_by_time[key] = {
      tx_hash           : tx.hash,
      tx_index          : tx.tx_index,
      'f:executed_time' : tx.executed_time,
      'f:ledger_index'  : tx.ledger_index,
      'f:type'          : tx.TransactionType,
      'f:result'        : tx.tx_result
    }

    //transactions by account sequence
    key = tx.Account + '|' + utils.padNumber(tx.Sequence, S_PAD);

    data.lu_account_transactions[key] = {
      tx_hash           : tx.hash,
      sequence          : tx.Sequence,
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
    tx['f:client']          = tx.client;

    delete tx.Account;
    delete tx.Sequence;
    delete tx.tx_result;
    delete tx.TransactionType;
    delete tx.executed_time;
    delete tx.ledger_index;
    delete tx.ledger_hash;
    delete tx.client;

    //add transaction
    data.transactions[tx.hash] = tx
  });

  return data;
};

/**
 * prepareParsedData
 */
HbaseClient.prototype.prepareParsedData = function (data) {
  var tables = {
    exchanges            : { },
    account_offers       : { },
    account_exchanges    : { },
    account_balance_changes : { },
    payments             : { },
    account_payments     : { },
    accounts_created     : { },
    memos                : { },
    lu_account_memos     : { },
    lu_affected_account_transactions : { },
    lu_account_offers_by_sequence : { },
  };

  //add exchanges
  data.exchanges.forEach(function(ex) {
    var suffix = utils.formatTime(ex.time) +
      '|' + utils.padNumber(ex.ledger_index, LI_PAD) +
      '|' + utils.padNumber(ex.tx_index, I_PAD) +
      '|' + utils.padNumber(ex.node_index, I_PAD); //guarantee uniqueness

    var key = ex.base.currency +
      '|' + (ex.base.issuer || '') +
      '|' + ex.counter.currency +
      '|' + (ex.counter.issuer || '') +
      '|' + suffix;

    var key2 = ex.buyer  + '|' + suffix;
    var key3 = ex.seller + '|' + suffix;
    var row  = {
      'f:base_currency'    : ex.base.currency,
      'f:base_issuer'      : ex.base.issuer || undefined,
      base_amount          : ex.base.amount,
      'f:counter_currency' : ex.counter.currency,
      'f:counter_issuer'   : ex.counter.issuer || undefined,
      counter_amount       : ex.counter.amount,
      rate                 : ex.rate,
      'f:buyer'            : ex.buyer,
      'f:seller'           : ex.seller,
      'f:taker'            : ex.taker,
      'f:provider'         : ex.provider,
      'f:offer_sequence'   : ex.sequence,
      'f:tx_hash'          : ex.tx_hash,
      'f:executed_time'    : ex.time,
      'f:ledger_index'     : ex.ledger_index,
      'f:tx_type'          : ex.tx_type,
      'f:client'           : ex.client,
      tx_index             : ex.tx_index,
      node_index           : ex.node_index
    };

    if (ex.autobridged) {
      row['f:autobridged_currency'] = ex.autobridged.currency;
      row['f:autobridged_issuer'] = ex.autobridged.issuer;
    }

    tables.exchanges[key] = row;
    tables.account_exchanges[key2] = row;
    tables.account_exchanges[key3] = row;
  });

  //add offers
  data.offers.forEach(function(o) {

    var key = o.account +
      '|' + utils.formatTime(o.executed_time) +
      '|' + utils.padNumber(o.ledger_index, LI_PAD) +
      '|' + utils.padNumber(o.tx_index, I_PAD) +
      '|' + utils.padNumber(o.node_index, I_PAD);

    tables.account_offers[key] = {
      'f:account'       : o.account,
      'f:sequence'      : o.sequence,
      'f:type'          : o.type,
      'f:pays_currency' : o.taker_pays.currency,
      'f:pays_issuer'   : o.taker_pays.issuer || undefined,
      pays_amount       : o.taker_pays.value,
      'f:gets_currency' : o.taker_gets.currency,
      'f:gets_issuer'   : o.taker_gets.issuer || undefined,
      gets_amount       : o.taker_gets.value,
      'f:expiration'    : o.expiration,
      'f:new_offer'     : o.new_offer,
      'f:old_offer'     : o.old_offer,
      'f:executed_time' : o.executed_time,
      'f:ledger_index'  : o.ledger_index,
      'f:client'        : o.client,
      tx_index          : o.tx_index,
      node_index        : o.node_index,
      tx_hash           : o.tx_hash
    }

    key = o.account +
      '|' + o.sequence +
      '|' + utils.padNumber(o.ledger_index, LI_PAD) +
      '|' + utils.padNumber(o.tx_index, I_PAD) +
      '|' + utils.padNumber(o.node_index, I_PAD);

    tables.lu_account_offers_by_sequence[o.account + '|' + o.sequence] = {
       'f:account'      : o.account,
      'f:sequence'      : o.sequence,
      'f:type'          : o.type,
      'f:executed_time' : o.executed_time,
      'f:ledger_index'  : o.ledger_index,
      tx_index          : o.tx_index,
      node_index        : o.node_index,
      tx_hash           : o.tx_hash
    }
  });

  //add balance changes
  data.balanceChanges.forEach(function(c) {
    var suffix = '|' + utils.formatTime(c.time) +
      '|' + utils.padNumber(c.ledger_index, LI_PAD) +
      '|' + utils.padNumber(c.tx_index, I_PAD) +
      '|' + (c.node_index === 'fee' ? '$' : utils.padNumber(c.node_index, I_PAD));

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
      node_index        : c.node_index,
      'f:client'        : c.client
    };

    key = c.account + suffix;
    tables.account_balance_changes[c.account + suffix] = row;

    //XRP has no issuer
    if (c.issuer) {
      tables.account_balance_changes[c.issuer  + suffix] = row;
    }
  });

  data.payments.forEach(function(p) {
    var key = utils.formatTime(p.time) +
      '|' + utils.padNumber(p.ledger_index, LI_PAD) +
      '|' + utils.padNumber(p.tx_index, I_PAD);

    var payment = {
      'f:source'          : p.source,
      'f:destination'     : p.destination,
      amount              : p.amount,
      delivered_amount    : p.delivered_amount,
      'f:currency'        : p.currency,
      'f:issuer'          : p.issuer,
      'f:source_currency' : p.source_currency,
      fee                 : p.fee,
      source_balance_changes      : p.source_balance_changes,
      destination_balance_changes : p.destination_balance_changes,
      'f:tx_hash'       : p.tx_hash,
      'f:executed_time' : p.time,
      'f:ledger_index'  : p.ledger_index,
      tx_index          : p.tx_index,
      'f:client'        : p.client
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
    tables.account_payments[p.source      + '|' + key] = payment;
    tables.account_payments[p.destination + '|' + key] = payment;
  });

  //add accounts created
  data.accountsCreated.forEach(function(a) {
    var key = utils.formatTime(a.time) +
      '|' + utils.padNumber(a.ledger_index, LI_PAD) +
      '|' + utils.padNumber(a.tx_index, I_PAD);

    tables.accounts_created[key] = {
      'f:account'       : a.account,
      'f:parent'        : a.parent,
      balance           : a.balance,
      'f:tx_hash'       : a.tx_hash,
      'f:executed_time' : a.time,
      'f:ledger_index'  : a.ledger_index,
      tx_index          : a.tx_index,
      'f:client'        : a.client
    };
  });

  //add memos
  data.memos.forEach(function(m) {
    var key = utils.formatTime(m.executed_time) +
      '|' + utils.padNumber(m.ledger_index, LI_PAD) +
      '|' + utils.padNumber(m.tx_index, I_PAD) +
      '|' + utils.padNumber(m.memo_index, I_PAD);

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
      'f:ledger_index'    : m.ledger_index,
      tx_index            : m.tx_index,
      memo_index          : m.memo_index
    };

    tables.lu_account_memos[m.account + '|' + key] = {
      rowkey            : key,
      'f:is_sender'     : true,
      'f:tag'           : m.source_tag,
      'f:tx_hash'       : m.tx_hash,
      'f:executed_time' : m.executed_time,
      'f:ledger_index'  : m.ledger_index,
      tx_index            : m.tx_index,
      memo_index          : m.memo_index
    };

    if (m.destination) {
      tables.lu_account_memos[m.destination + '|' + key] = {
        rowkey            : key,
        'f:is_source'     : false,
        'f:tag'           : m.destination_tag,
        'f:tx_hash'       : m.tx_hash,
        'f:executed_time' : m.executed_time,
        'f:ledger_index'  : m.ledger_index,
        tx_index            : m.tx_index,
        memo_index          : m.memo_index
      };
    }
  });

  //add affected accounts
  data.affectedAccounts.forEach(function(a) {
    var key = a.account +
      '|' + utils.formatTime(a.time) +
      '|' + utils.padNumber(a.ledger_index, LI_PAD) +
      '|' + utils.padNumber(a.tx_index, I_PAD);

    tables.lu_affected_account_transactions[key] = {
      'f:type'          : a.tx_type,
      'f:result'        : a.tx_result,
      tx_hash           : a.tx_hash,
      tx_index          : a.tx_index,
      'f:executed_time' : a.time,
      'f:ledger_index'  : a.ledger_index,
      'f:client'        : a.client
    }
  });

  return tables;
};

/**
 * SaveParsedData
 */

HbaseClient.prototype.saveParsedData = function (params, callback) {
  var self = this;
  var tables = self.prepareParsedData(params.data);
  var tableNames;

  tableNames = params.tableNames ? params.tableNames : Object.keys(tables);

  Promise.map(tableNames, function(name) {
    return self.putRows(name, tables[name]);
  })
  .nodeify(function(err, resp) {
    var total = 0;
    if (err) {
      self.log.error('error saving parsed data', err);
    } else {
      if (resp) {
        resp.forEach(function(r) {
          if (r && r[0]) total += r[0];
        });
      }

      self.log.info('parsed data saved:', total + ' rows');
    }

    if (callback) {
      callback(err, total);
    }
  });
};

/**
 * removeLedger
 */

HbaseClient.prototype.removeLedger = function (hash, callback) {
  var self = this;

  self.getLedger({
    ledger_hash: hash,
    transactions: true,
    expand: true,
    invalid: true

  }, function(err, ledger) {
    var parsed;
    var primary;
    var secondary;
    var transactions;
    var tables;
    var table;

    if (err) {
      self.log.error('error fetching ledger:', hash, err);
      callback(err);
      return;
    }

    if (!ledger) {
      callback('ledger not found');
      return;
    }

    //parser expects ripple epoch
    ledger.close_time  -= EPOCH_OFFSET;
    transactions        = ledger.transactions;
    ledger.transactions = [];

    //ledgers must be formatted according to the output from
    //rippled's ledger command
    transactions.forEach(function(tx, i) {
      if (!tx) return;

      var transaction      = tx.tx;
      transaction.metaData = tx.meta,
      transaction.hash     = tx.hash,
      ledger.transactions.push(transaction);
    });

    parsed       = Parser.parseLedger(ledger);
    primary      = self.prepareLedgerTables(ledger);
    secondary    = self.prepareParsedData(parsed);
    transactions = self.prepareTransactions(parsed.transactions);
    tables       = [ ];

    for (table in primary) {
      tables.push({
        table : table,
        keys  : Object.keys(primary[table])
      });
    }

    for (table in transactions) {
      tables.push({
        table : table,
        keys  : Object.keys(transactions[table])
      });
    }

    for (table in secondary) {
      tables.push({
        table : table,
        keys  : Object.keys(secondary[table])
      });
    }

    Promise.map(tables, function(t) {
      return self.deleteRows(t.table, t.keys);
    }).nodeify(function(err, resp) {
      self.log.info('ledger removed:', ledger.ledger_index, hash);
      callback(err, resp);
    });
  });
};

module.exports = HbaseClient;
