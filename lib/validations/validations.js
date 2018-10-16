'use strict';
var config = require('../../config')
const dateFormat = 'YYYY-MM-DDTHH:mm:ss.SSS[Z]';
const Promise = require('bluebird');
const CronJob = require('cron').CronJob;
const Logger = require('../logger');
const log = new Logger({scope: 'validations'});
const smoment = require('../smoment');
const hbase = require('../hbase');
const nconf = require('nconf');

/**
 * Validations
 */

const Validations = function() {

  var getRows = Promise.promisify(hbase.getRows);
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
      validation.validation_public_key,
      validation.date.hbaseFormatStartRow()
    ].join('|');

    rows.push({
      table: 'validations_by_ledger',
      rowkey: key,
      columns: {
        reporter_public_key: validation.reporter_public_key,
        validation_public_key: validation.validation_public_key,
        amendments: validation.amendments,
        base_fee: validation.base_fee,
        flags: validation.flags,
        full: validation.full,
        ledger_index: validation.ledger_index,
        load_fee: validation.load_fee,
        ledger_hash: validation.ledger_hash,
        reserve_base: validation.reserve_base,
        reserve_inc: validation.reserve_inc,
        signature: validation.signature,
        signing_time: validation.signing_time,
        first_datetime: validation.timestamp,
        last_datetime: validation.timestamp,
        count: validation.count
      }
    });

    rows.push({
      table: 'validator_state',
      rowkey: validation.validation_public_key,
      columns: {
        pubkey: validation.validation_public_key,
        last_ledger_time: validation.timestamp,
        current_index: validation.ledger_index,
        current_hash: validation.leedger_hash
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

  function updateValidation(validation) {

    const key = [
      validation.ledger_hash,
      validation.validation_public_key,
      validation.date.hbaseFormatStartRow()
    ].join('|');

    hbase.putRow({
      table: 'validations_by_ledger',
      rowkey: key,
      columns: {
        last_datetime: validation.last_datetime,
        count: validation.count
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
    });
  }

  function recursiveUpdateReports() {
    updateReports()
    .then(function() {
      setTimeout(recursiveUpdateReports, 60 * 1000)
    })
  }


  return {
    updateReports: updateReports,

    /**
     * start
     * set purge interval
     * and load historical data
     */

    start: function() {
      setInterval(purge, 30 * 1000);
      setInterval(function() {
        log.info('hbase connections:', hbase.pool.length);
      }, 60 * 1000);

      // continously update reports
      recursiveUpdateReports()
    },

    /**
     * handleValidation
     */

    handleValidation: function(data) {
      const validation = {
        reporter_public_key: data.reporter_public_key,
        validation_public_key: data.validation_public_key,
        amendments: data.amendments,
        base_fee: data.base_fee,
        ephemeral_public_key: data.ephemeral_public_key,
        flags: data.flags,
        full: data.full,
        ledger_index: data.ledger_index,
        load_fee: data.load_fee,
        ledger_hash: data.ledger_hash,
        reserve_base: data.reserve_base,
        reserve_inc: data.reserve_inc,
        signature: data.signature,
        signing_time: data.signing_time,
        date: smoment(),
        timestamp: smoment().format(dateFormat),
        count: 1
      };

      const key = [
        validation.ledger_hash,
        validation.validation_public_key
      ].join('|');

      return new Promise((resolve, reject) => {
        if (!validation.validation_public_key) {
          return reject('validation_public_key cannot be null');
        } else if (!validation.ledger_hash) {
          return reject('ledger_hash cannot be null');
        } else if (!validation.flags) {
          return reject('flags cannot be null');
        } else if (!validation.signing_time) {
          return reject('signing_time cannot be null');
        } else if (!validation.signature) {
          return reject('signature cannot be null');
        }

        // already encountered
        if (validations[key]) {
          validations[key].count++;
          validations[key].last_datetime = validation.timestamp;

          clearTimeout(validations[key].debounce);
          validations[key].debounce = setTimeout(updateValidation.bind(this),
                                                 1000, validations[key]);
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
              .catch(function(e) {ha
                reject(e);
              });
            } else {
              log.info(('duplicate: ' + resp).grey);
              resolve(key);
            }
          });
        }
      });
    }
  };
};

module.exports = Validations;
