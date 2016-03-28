'use strict';

const smoment = require('../smoment');
const Promise = require('bluebird');
const Logger = require('../logger');
const log = new Logger({scope : 'validator reports'});
const colors = require('colors');

const clusters = {
  alt: [
    'n9LiNzfbTN5wEc9j2CM9ps7gQqAusVz8amg4gnsfHZ3DWHr2kkG1',
    'n94a8g8RVLQR3jTRJRatdSvWM7JYmeH433jizBHFaezPVWendSoo',
    'n94QR9qmtF31xApcc9d5KRy2CcSjezBDRaAfEGzHAWZxpMisFedR',
    'n9MQeSow2qkAvqQBpWh1EijEwhHez56mB3B5yLVYFA4UpDquKwzA',
    'n9LYyd8eUVd54NQQWPAJRFPM1bghJjaf1rkdji2haF4zVjeAPjT2'
  ],
  main: [
    'n949f75evCHwgyP4fPVgaHqNHxUVN15PsJEZ3B3HnXPcPjcZAoy7',
    'n9MD5h24qrQqiyBC8aeqqCWvpiBiYQ3jxSr91uiDvmrkyHRdYLUj',
    'n9L81uNCaPgtUJfaHh89gmdvXKAmSt5Gdsw2g1iPWaPkAHW5Nm4C',
    'n9KiYM9CgngLvtRCQHZwgC2gjpdaZcCcbt3VboxiNFcKuwFVujzS',
    'n9LdgEtkmGB9E2h3K4Vp7iGUaKuq23Zr32ehxiU8FWY7xoxbWTSA'
  ]
};

module.exports = hbase => {

  /**
   * isClusterLedger
   */

  function isClusterLedger(name, ledger) {
    const cluster = clusters[name];
    let count = 0;
    let pubkey;

    for (pubkey in ledger.validators) {
      if (cluster.indexOf(pubkey) !== -1) {
        count++;
      }
    }

    return count >= 3;
  }

  /**
   * calculateAgreement
   */

  function caclulateAgreement(count, clusterCount) {
    let agreement = 0;

    if (clusterCount) {
      agreement = count / clusterCount;
    }

    return agreement.toPrecision(5);
  }

  /**
   * getValidations
   */

  function getValidations(start, end) {
    const validations = [];

    function getBatch(marker, callback) {
      hbase.getScanWithMarker(hbase, {
        table: 'validations_by_date',
        startRow: start.hbaseFormatStartRow(),
        stopRow: end.hbaseFormatStopRow(),
        marker: marker,
        limit: 10000,
        filterString: 'KeyOnlyFilter()',
        descending: false
      }, (err, resp) => {
        if (err) {
          callback(err);
        } else {

          // append to the list
          validations.push.apply(validations, resp.rows);

          if (resp.marker) {
            getBatch(resp.marker, callback);
          } else {
            callback();
          }
        }
      });
    }

    return new Promise((resolve, reject) => {
      log.info('Getting validations...');
      getBatch(null, err => {
        if (err) {
          reject(err);
        } else {
          log.info('validations found:', validations.length.toString().bold);
          resolve(validations);
        }
      });
    });
  }

  /**
   * processValidations
   */

  function processValidations(start, validations) {

    const ledgers = {};
    const validators = {};
    let r;

    validations.forEach(row => {
      r = row.rowkey.split('|');
      r = {
        validation_public_key: r[1],
        ledger_hash: r[2]
      };

      if (!ledgers[r.ledger_hash]) {
        ledgers[r.ledger_hash] = {
          validators: {}
        };
      }

      ledgers[r.ledger_hash].validators[r.validation_public_key] = true;

      if (!validators[r.validation_public_key]) {
        validators[r.validation_public_key] = {
          validation_public_key: r.validation_public_key,
          date: start.format(),
          total_ledgers: 0,
          main_net_ledgers: 0,
          alt_net_ledgers: 0,
          other_ledgers: 0
        };
      }
    });

    return {
      ledgers: ledgers,
      validators: validators
    };
  }

  /**
   * createReports
   */

  function createReports(data) {

    let totalMainLedgers = 0;
    let totalAltLedgers = 0;
    let ledger;
    let validator;
    let key;

    function updateValidators(l) {
      let pubkey;

      for (pubkey in l.validators) {
        data.validators[pubkey].total_ledgers++;
        data.validators[pubkey].main_net_ledgers += l.isMain ? 1 : 0;
        data.validators[pubkey].alt_net_ledgers += l.isAlt ? 1 : 0;
        data.validators[pubkey].other_ledgers +=
          (!l.isAlt && !l.isMain) ? 1 : 0;
      }
    }

    for (key in data.ledgers) {
      ledger = data.ledgers[key];
      if (isClusterLedger('main', ledger)) {
        ledger.isMain = true;
        totalMainLedgers++;
      }

      if (isClusterLedger('alt', ledger)) {
        ledger.isAlt = true;
        totalAltLedgers++;
      }

      updateValidators(ledger);
    }

    for (key in data.validators) {
      validator = data.validators[key];
      validator.main_net_agreement =
        caclulateAgreement(validator.main_net_ledgers, totalMainLedgers);
      validator.alt_net_agreement =
        caclulateAgreement(validator.alt_net_ledgers, totalAltLedgers);
    }

    log.info('main net ledgers:'.cyan, totalMainLedgers.toString().cyan.bold);
    log.info('alt net ledgers:'.cyan, totalAltLedgers.toString().cyan.bold);
    log.info('total ledgers:'.cyan, Object.keys(data.ledgers).length.toString().cyan.bold);
    return data.validators;
  }

  /**
   * saveReports
   */

  function saveReports(validators) {

    return Promise.map(Object.keys(validators), pubkey => {
      const v = validators[pubkey];
      const rowkey = [
        smoment(v.date).hbaseFormatStartRow(),
        v.validation_public_key
      ].join('|');

      return hbase.putRow({
        table: 'validator_reports',
        rowkey: rowkey,
        columns: v
      });
    }).then(() => {
      log.info((Object.keys(validators).length + ' reports saved.').green);
    });
  }

  return {
    generateReports: date => {
      const start = smoment(date);
      const end = smoment(date);

      start.moment.startOf('day');
      end.moment.startOf('day').add(1, 'day');

      log.info('Generating reports for ' + start.format('YYYY-MM-DD').magenta);
      return getValidations(start, end)
      .then(processValidations.bind(this, start))
      .then(createReports)
      .then(saveReports);
    }
  };
};
