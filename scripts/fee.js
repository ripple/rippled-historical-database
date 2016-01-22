var config  = require('../config/import.config');
var Logger  = require('../lib/logger');
var Hbase   = require('../lib/hbase/hbase-client');
var Parser  = require('../lib/ledgerParser');
var utils   = require('../lib/utils');
var Promise = require('bluebird');
var smoment = require('../lib/smoment');
var binary = require('ripple-binary-codec');
var options = config.get('hbase');

options.logLevel = 1;

var hbase = new Hbase(options);
var count = 0;

function getNext(marker) {
  var start = smoment(0).hbaseFormatStartRow();
  var stop = smoment().hbaseFormatStopRow();

  hbase.getScanWithMarker(hbase, {
    table: 'lu_transactions_by_time',
    startRow: start,
    stopRow: stop,
    marker: marker,
    limit: 10,
    descending: false
  }, function(err, resp) {
    if (err) {
      console.log(err);
      console.log(marker, count);
      process.exit();
    } else {
      Promise.map(resp.rows, updateFee)
      .then(function() {
        if (resp.marker) {
          console.log(resp.marker, count);
          getNext(resp.marker);
        } else {
          process.exit();
        }

      }).catch(function(e) {
        console.log(e);
        console.log(marker, count);
        process.exit();
      });
    }
  });
}

function updateFee(d) {
  return new Promise(function(resolve, reject) {
    hbase.getTransaction({
      tx_hash: d.tx_hash,
      binary: true
    }, function(err, tx) {
      var tx_json = binary.decode(tx.tx);
      hbase.putRow('transactions', d.tx_hash, {Fee:tx_json.Fee})
      .then(function(resp) {
        count++;
        resolve(resp);
      }).catch(function(e) {
        reject(e);
      });
    });
  });
}


getNext(config.get('marker'));
