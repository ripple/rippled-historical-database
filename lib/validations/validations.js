'use strict';

const dateFormat = 'YYYY-MM-DDTHH:mm:ss.SSS[Z]';
const Promise = require('bluebird');
const smoment = require('../smoment');
const CronJob = require('cron').CronJob;
const Verifier = require('ripple-domain-verifier');
const verifier = new Verifier();
const Hbase = require('../hbase/hbase-client');
const Logger = require('../logger');
const log = new Logger({scope: 'validations'});

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
        validation.date.hbaseFormatStartRow(true),
        validation.validation_public_key,
        validation.ledger_hash
      ].join('|'),
      columns: {
        validation_public_key: validation.validation_public_key,
        ledger_hash: validation.ledger_hash,
        datetime: validation.timestamp
      }
    });

    rows.push({
      table: 'validators',
      rowkey: validation.validation_public_key,
      columns: {
        validation_public_key: validation.validation_public_key,
        last_datetime: validation.timestamp
      }
    });

    return Promise.map(rows, function(row) {
      return hbase.putRow(row);
    }).catch(e => {
      log.error(e.toString().red);
    });
  }

  function verifyDomains() {
    return hbase.getValidators()
    .then(validators => {
      return Promise.map(validators, function(v) {
        return verifyDomain(v);
      })
      .then(() => {
        log.info('domain verification complete'.cyan);
      }).catch(e => {
        log.error('verify domains', e.toString().red);
      });
    });
  }

  /**
   * updateDomain
   */

  function updateDomain (params) {
    return hbase.putRow({
      table: 'validators',
      rowkey: params.pubkey,
      columns: {
        domain: params.domain,
        domain_state: params.state
      }
    }).then(() => {
      const date = smoment();

      return hbase.putRow({
        table: 'validator_domain_changes',
        rowkey: date.hbaseFormatStartRow() + '|' + params.pubkey,
        columns: {
          validation_public_key: params.pubkey,
          domain: params.domain,
          domain_state: params.state,
          date: date.format()
        }
      }).then(() => {
        log.info('updated domain info'.magenta,
                 params.pubkey.cyan, params.state.grey, (params.domain || '').magenta.dim.underline);
      });
    });
  }

  function verifyDomain(validator) {
    return new Promise(function(resolve, reject) {
      hbase.getRow({
        table: 'manifests_by_master_key',
        rowkey: validator.validation_public_key
      }, function(err, resp) {
        if (err) {
          reject(err);

        } else {
          const pubkey = resp ?
                resp.ephemeral_public_key : validator.validation_public_key;
          const master = resp ?
                validator.validation_public_key : undefined;

          return verifier.verifyValidatorDomain(pubkey, master)
          .then(domain => {
            log.info(domain.green, master || pubkey);
            if (domain !== validator.domain ||
                validator.domain_state !== 'verified') {
              updateDomain({
                pubkey: validator.validation_public_key,
                domain: domain,
                state: 'verified'
              })
              .then(resolve)
              .catch(reject);
            } else {
              resolve();
            }
          }).catch(e => {
            log.info(e.type.red, e.message.yellow, master || pubkey);
            if (e.type !== validator.domain_state ||
               (e.type !== 'AccountDomainNotFound' &&
                e.message !== validator.domain)) {
              updateDomain({
                pubkey: validator.validation_public_key,
                domain: e.type === 'AccountDomainNotFound' ? undefined : e.message,
                state: e.type
              })
              .then(resolve)
              .catch(reject);
            } else {
              resolve();
            }
          });
        }
      });
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

  return {
    updateReports: updateReports,
    verifyDomains: verifyDomains,

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

      // setup cron job for updating reports
      const reportsCron = new CronJob({
        cronTime: '0 */10 * * * *',
        onTick: updateReports,
        start: true
      });

      // setup cron job for domain verification
      const verifyCron = new CronJob({
        cronTime: '0 0 * * * *',
        onTick: verifyDomains,
        start: true
      });

      verifyDomains();
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
      });
    }
  };
};

module.exports = Validations;
