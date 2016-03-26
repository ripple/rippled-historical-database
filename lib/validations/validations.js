'use strict';

const config = require('../../config/import.config');
const dateFormat = 'YYYY-MM-DDTHH:mm:ss.SSS[Z]';
const Promise = require('bluebird');
const smoment = require('../smoment');
const CronJob = require('cron').CronJob;
const Hbase = require('../hbase/hbase-client');
const hbaseOptions = config.get('hbase');

hbaseOptions.logLevel = 2;

const hbase = new Hbase(hbaseOptions);
const reports = require('./reports')(hbase);
const validations = {};

/**
 * saveValidation
 * save data to hbase
 */

function saveValidation(validation) {
  const rows = [];
  const key = [
    validation.ledger_hash,
    validation.validation_public_key
  ].join('|');

  rows.push({
    table: 'validations_by_ledger',
    rowkey: key,
    columns: {
      reporter_public_key: validation.reporter_public_key,
      validation_public_key: validation.validation_public_key,
      ledger_hash: validation.ledger_hash,
      signature: validation.signature,
      first_datetime: validation.timestamp,
      last_datetime: validation.timestamp,
      count: validation.count
    }
  });

  rows.push({
    table: 'validations_by_validator',
    rowkey: [
      validation.validation_public_key,
      validation.date.hbaseFormatStartRow(),
      validation.ledger_hash
    ].join('|'),
    columns: {
      validation_public_key: validation.validation_public_key,
      ledger_hash: validation.ledger_hash,
      datetime: validation.timestamp
    }
  });

  rows.push({
    table: 'validations_by_date',
    rowkey: [
      validation.date.hbaseFormatStartRow(),
      validation.validation_public_key,
      validation.ledger_hash
    ].join('|'),
    columns: {
      validation_public_key: validation.validation_public_key,
      ledger_hash: validation.ledger_hash,
      datetime: validation.timestamp
    }
  });

  return Promise.map(rows, function(row) {
    return hbase.putRow(row);
  }).catch(e => {
    console.log(e.toString().red);
  });
}

/**
 * updateValidation
 */

  function updateValidation(key, validation) {
    hbase.putRow({
      table: 'validations_by_ledger',
      rowkey: key,
      columns: {
        last_datetime: validation.last_datetime,
        count: validations.count
      }
    }).catch(e => {
      console.log(e.toString().red);
    });
  }

/**
 * handleValidation
 */

function handleValidation(data) {

  const validation = {
    reporter_public_key: data.reporter_public_key,
    validation_public_key: data.validation_public_key,
    ledger_hash: data.ledger_hash,
    signature: data.signature,
    date: smoment(),
    timestamp: smoment().format(dateFormat),
    count: 1
  };

  const key = [
    validation.ledger_hash,
    validation.validation_public_key
  ].join('|');

  // already encountered
  if (validations[key]) {
    validations[key].count++;
    validations[key].last_datetime = validation.timestamp;

    clearTimeout(validations[key].debounce);
    validations[key].debounce = setTimeout(updateValidation.bind(this),
                                           1000, key, validations[key]);

  // first encounter
  } else {
    validations[key] = validation; // cache

    // check for row first to
    // avoid duplicates on the
    // secondary tables
    hbase.getRow({
      table: 'validations_by_ledger',
      rowkey: key,
      columns: ['d:first_datetime']
    }, function(err, resp) {
      if (err) {
        console.log(err.toString().red);

      } else if (!resp) {
        saveValidation(validation)
        .catch(function(e) {
          console.log(key.red + e.toString().red);
        });
      } else {
        console.log(('duplicate: ' + resp).grey);
      }
    });
  }
}

/**
 * purge
 * purge cached data
 */

function purge() {
  const now = smoment();
  const maxTime = 5 * 60 * 1000;
  let key;
  let count;

  for (key in validations) {
    if (now.moment.diff(validations[key].timestamp) > maxTime) {
      delete validations[key];
    }
  }

  count = Object.keys(validations).length;
  console.log(('cached validations: ' + count).green);
}

function updateReports() {
  const date = smoment();

  date.moment.subtract(10, 'minute');
  reports.generateReports(date);
}

/**
 * start
 * set purge interval
 * and load historical
 * data
 */

function start() {
  setInterval(purge, 30 * 1000);
  setInterval(function() {
    console.log('hbase connections:', hbase.pool.length);
  }, 60 * 1000);

  // setup cron job
  const job = new CronJob({
    cronTime: '0 */10 * * * *',
    onTick: updateReports,
    start: true
  });
}

module.exports = {
  handleValidation: handleValidation,
  start: start
};
