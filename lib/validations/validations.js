'use strict';

const dateFormat = 'YYYY-MM-DDTHH:mm:ss.SSS[Z]';
const Promise = require('bluebird');
const smoment = require('../smoment');
const CronJob = require('cron').CronJob;
const Hbase = require('../hbase/hbase-client');
const Logger = require('../logger');
const log = new Logger({scope : 'validations'});

/**
 * Validations
 */

const Validations = function(config) {

  config.logLevel = 2;

  const hbase = new Hbase(config);
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
      log.error(e.toString().red);
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
      log.error(e.toString().red);
    });
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
    log.info(('cached validations: ' + count).green);
  }

  function updateReports(timestamp) {
    const date = smoment(timestamp);

    if (!timestamp) {
      date.moment.subtract(10, 'minute');
    }

    return reports.generateReports(date)
    .catch(e => {
      log.error((e.stack || e).toString().red);
    });;
  }

  /**
   * start
   * set purge interval
   * and load historical
   * data
   */

  return {
    updateReports : updateReports,

    start: function() {
      setInterval(purge, 30 * 1000);
      setInterval(function() {
        log.info('hbase connections:', hbase.pool.length);
      }, 60 * 1000);

      // setup cron job
      const job = new CronJob({
        cronTime: '0 */10 * * * *',
        onTick: updateReports,
        start: true
      });
    },

    /**
     * handleValidation
     */

    handleValidation: function(data) {
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

      return new Promise((resolve, reject) => {

        // already encountered
        if (validations[key]) {
          validations[key].count++;
          validations[key].last_datetime = validation.timestamp;

          clearTimeout(validations[key].debounce);
          validations[key].debounce = setTimeout(updateValidation.bind(this),
                                                 1000, key, validations[key]);
          resolve();

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
              reject(err);

            } else if (!resp) {
              saveValidation(validation)
              .then(() => {
                resolve(key);
              })
              .catch(function(e) {
                reject(e);
              });
            } else {
              log.info(('duplicate: ' + resp).grey);
              resolve(key);
            }
          });
        }
      })
    }
  }
}

module.exports = Validations;
