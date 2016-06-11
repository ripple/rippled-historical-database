var config = require('../config/import.config');
var request = require('request-promise');
var WebSocket = require('ws');
var Logger = require('../lib/logger');
var log = new Logger({scope : 'fee etl'});
var colors = require('colors');
var smoment = require('../lib/smoment');
var CronJob = require('cron').CronJob;
var Hbase = require('../lib/hbase/hbase-client');
var hbase = new Hbase(config.get('hbase'));
var nodemailer = require('nodemailer');
var transporter = nodemailer.createTransport();
var QUEUE_THRESHOLD = config.get('queue_threshold') || 80;
var inactive = true;

log.info(('queue threshold: ' + QUEUE_THRESHOLD + '%').red);

/**
 * dropsToXRP
 */

function dropsToXRP(d) {
  return Number(d) / 1000000;
}

/**
 * getFeeStats
 */

function getFeeStats() {
  var date = smoment();

  return request({
    url: config.get('fee_url'),
    timeout: 3000,
    json: {
      method: 'fee',
      params: [{}]
    }
  })
  .then(function(d) {
    if (d.result.status === 'error') {
      throw new Error(result.error_message);
    }

    // max queue size is 20x expected ledger size
    var max = Number(d.result.expected_ledger_size) * 20;
    var pct = Number(d.result.current_queue_size) / max * 100;

    return {
      date: date,
      data: {
        date: date.format(),
        current_ledger_size: Number(d.result.current_ledger_size),
        expected_ledger_size: Number(d.result.expected_ledger_size),
        current_queue_size: Number(d.result.current_queue_size),
        pct_max_queue_size: pct.toFixed(2),
        minimum_fee: dropsToXRP(d.result.drops.minimum_fee),
        open_ledger_fee: dropsToXRP(d.result.drops.open_ledger_fee),
        median_fee: dropsToXRP(d.result.drops.median_fee)
      }
    };
  });
}

/**
 * saveFeeStats
 */

function saveFeeStats(d) {
  return hbase.putRow({
    table: 'fee_stats',
    rowkey: 'raw|' + d.date.hbaseFormatStartRow(),
    columns: d.data
  }).then(function() {
    log.info(d.date.format(),
             ('ledger size:' + d.data.current_ledger_size).cyan,
             (d.data.pct_max_queue_size + '% of max').green);
    return d;
  });
}

/**
 * checkAlerts
 */

function checkAlerts(d) {
  var recipients = config.get('recipients');
  var pct = Number(d.data.pct_max_queue_size);
  var params = {};

  if (inactive && pct >= QUEUE_THRESHOLD && recipients) {

    //limit notifications to
    //no more than 1 every 5 minutes
    inactive = false;
    setTimeout(function(d) {
      inactive = true;
    }, 5 * 60 * 1000);

    params.from = 'Ripple Fee Notification <notify@ripple.com>';
    params.to = recipients;
    params.subject = 'Pct max queue exeeded threshold: ' + pct + '%';
    params.html = 'The current que size exceeded ' + QUEUE_THRESHOLD + '%' +
      ' of the max threshold size.<br>' +
      '<ul><li>Date: ' + d.date.format('LLLL z') + '</li>' +
      '<li>percent of max queue size: ' + pct + '%</li>' +
      '<li>current queue size: ' + d.data.current_queue_size + '</li>' +
      '<li>expected ledger size: ' + d.data.expected_ledger_size + '</li>' +
      '<ul>';

    return new Promise(function(resolve, reject) {
      transporter.sendMail(params, function(err, info) {
        if (err) {
          reject(err);
        } else {
          log.info('Notification sent: ',
                   pct + '%',
                   '(threshold:' + QUEUE_THRESHOLD + '%)');
          resolve(d);
        }
      });
    });
  } else {
    return d;
  }
}

/**
 * aggregate
 */

function aggregate(d) {
  return d;
}

/**
 * importFeeStats
 */

function importFeeStats() {
  return getFeeStats()
  .then(saveFeeStats)
  .then(checkAlerts)
  .then(aggregate)
  .catch(function(e) {
    console.log(e);
    console.log(e.stack);
  })
}

// setup cron job
var cron = new CronJob({
  cronTime: '*/5 * * * * *',
  onTick: importFeeStats,
  start: true
});
