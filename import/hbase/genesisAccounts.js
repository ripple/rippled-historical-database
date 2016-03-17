'use strict';
var config = require('../../config/import.config');
var Hbase = require('../../lib/hbase/hbase-client');
var utils = require('../../lib/utils');
var BigNumber = require('bignumber.js');
var moment = require('moment');
var ledger = require('../../lib/32570.json');
var hbase = new Hbase(config.get('hbase'));
var count = 0;
var time = utils.formatTime(ledger.close_time_human);
var unix = moment.utc(ledger.close_time_human).unix();

ledger.accountState.forEach(function(state, i) {
  var row;
  var rowkey;

  if (state.LedgerEntryType === 'AccountRoot') {
    count++;
    row = {
      'f:account' : state.Account,
      'f:genesis_balance' : new BigNumber(state.Balance).dividedBy(1000000).toString(),
      'f:executed_time' : unix,
      'f:ledger_index' : ledger.ledger_index,
      'd:genesis_index' : count
    };

    rowkey = time + '|' + utils.padNumber(ledger.ledger_index, 12) + '|genesis|' + utils.padNumber(count, 3);
    hbase.putRow({
      table: 'accounts_created',
      rowkey: rowkey,
      columns: row
    })
    .nodeify(function(err, resp) {
      console.log(rowkey, err, resp);
    });
  }
});

console.log(count + ' Accounts');
