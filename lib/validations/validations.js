'use strict';

const config = require('../../config/import.config');
const dateFormat = 'YYYY-MM-DDTHH:mm:ss.SSS[Z]';
const smoment = require('../smoment');
const Hbase = require('../hbase/hbase-client');
const hbase = new Hbase(config.get('hbase'));
const Promise = require('bluebird');

const clusters = {
  alt: [
    'n9L21JCXxZzPKshzEdUueJVViqYWHeAERmBAcPCB8op2SXKXMxyZ',
    'n94QR9qmtF31xApcc9d5KRy2CcSjezBDRaAfEGzHAWZxpMisFedR',
    'n9KkJSm36q8oEWGAUf9KiSxPEKvMT9VKEkdNqhATyoLwGEL7aGM9',
    'n94a8g8RVLQR3jTRJRatdSvWM7JYmeH433jizBHFaezPVWendSoo',
    'n9MQeSow2qkAvqQBpWh1EijEwhHez56mB3B5yLVYFA4UpDquKwzA'
  ],
  main: [
    'n949f75evCHwgyP4fPVgaHqNHxUVN15PsJEZ3B3HnXPcPjcZAoy7',
    'n9MD5h24qrQqiyBC8aeqqCWvpiBiYQ3jxSr91uiDvmrkyHRdYLUj',
    'n9L81uNCaPgtUJfaHh89gmdvXKAmSt5Gdsw2g1iPWaPkAHW5Nm4C',
    'n9KiYM9CgngLvtRCQHZwgC2gjpdaZcCcbt3VboxiNFcKuwFVujzS',
    'n9LdgEtkmGB9E2h3K4Vp7iGUaKuq23Zr32ehxiU8FWY7xoxbWTSA'
  ]
};

const validations = {};
const validators = {};
const ledgers = {};
const clusterLedgers = {
  main: {},
  alt: {}
};

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
    data: {
      reporter_public_key: validation.reporter_public_key,
      validation_public_key: validation.validation_public_key,
      ledger_hash: validation.ledger_hash,
      signature: validation.signature,
      first_datetime: validation.timestamp,
      last_datetime: validation.timestamp
    }
  });

  rows.push({
    table: 'validations_by_validator',
    rowkey: [
      validation.validation_public_key,
      validation.date.hbaseFormatStartRow(),
      validation.ledger_hash
    ].join('|'),
    data: {
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
    data: {
      validation_public_key: validation.validation_public_key,
      ledger_hash: validation.ledger_hash,
      datetime: validation.timestamp
    }
  });

  rows.push({
    table: 'validators_by_reporter',
    rowkey: [
      validation.reporter_public_key,
      validation.validation_public_key
    ].join('|'),
    data: {
      reporter_public_key: validation.reporter_public_key,
      validation_public_key: validation.validation_public_key,
      last_datetime: validation.timestamp
    }
  });

  return Promise.map(rows, function(row) {
    return hbase.putRow(row.table, row.rowkey, row.data);
  }).catch(e => {
    console.log(e);
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
    timestamp: smoment().format(dateFormat)
  };

  const key = [
    validation.ledger_hash,
    validation.validation_public_key
  ].join('|');

  // already encountered
  if (validations[key]) {

    // update the last datetime
    hbase.putRow('validations_by_ledger', key, {
      last_datetime: validation.timestamp
    }).catch(e => {
      console.log(e);
    });


  // first encounter
  } else {
    validations[key] = validation; // cache

    if (!ledgers[validation.ledger_hash]) {
      ledgers[validation.ledger_hash] = {
        timestamp: validation.date.format(),
        validators: []
      };
    }

    // cache ledger validators
    ledgers[validation.ledger_hash]
    .validators.push(validation.validation_public_key);

    saveValidation(validation)
    .catch(function(e) {
      console.log(key, e);
    });
  }
}

/**
 * isClusterLedger
 */

function isClusterLedger(name, ledger) {
  const cluster = clusters[name];
  let count = 0;

  ledger.validators.forEach(v => {
    if (cluster.indexOf(v) !== -1) {
      count++;
    }
  });

  return count >= 3;
}

/**
 * updateClusterLedgers
 */

function updateClusterLedgers(row) {
  const key = [
    smoment(row.date).hbaseFormatStartRow(),
    row.cluster
  ].join('|');

  return hbase.putRow('cluster_ledgers', key, row);
}

/**
 * updateReports
 * update clusterLedgers
 * and validator reports
 * from an incoming ledger
 */

function updateReports(ledger) {
  let date = smoment(ledger.timestamp);
  date.moment.startOf('day');
  date = date.format();

  if (!clusterLedgers.main[date]) {
    clusterLedgers.main[date] = 0;
  }

  if (!clusterLedgers.alt[date]) {
    clusterLedgers.alt[date] = 0;
  }

  // main net cluster ledgers
  if (isClusterLedger('main', ledger)) {
    ledger.isMain = true;
    clusterLedgers.main[date]++;
    updateClusterLedgers({
      cluster: 'main',
      date: smoment(date).format(),
      count: clusterLedgers.main[date]
    });
  }

  // alt net cluster ledgers
  if (isClusterLedger('alt', ledger)) {
    ledger.isAlt = true;
    clusterLedgers.alt[date]++;
    updateClusterLedgers({
      cluster: 'alt',
      date: smoment(date).format(),
      count: clusterLedgers.alt[date]
    });
  }

  // update validator reports
  ledger.validators.forEach(v => {
    if (!validators[v]) {
      validators[v] = {};
    }

    if (!validators[v][date]) {
      validators[v][date] = {
        validator: v,
        date: date,
        total_ledgers: 0,
        main_net_ledgers: 0,
        alt_net_ledgers: 0,
        other_ledgers: 0
      };
    }

    const report = validators[v][date];


    report.total_ledgers++;
    report.main_net_ledgers +=
      ledger.isMain ? 1 : 0;
    report.alt_net_ledgers +=
      ledger.isAlt ? 1 : 0;
    report.other_ledgers +=
      !ledger.isAlt &&
      !ledger.isMain ? 1 : 0;
  });
}

/**
 * calculateAgreement
 */

function caclulateAgreement(count, clusterCount) {
  let agreement = 0;

  if (clusterCount) {
    agreement = count / clusterCount;
  }

  // disallow greater than 1
  if (agreement > 1) {
    agreement = 1;
  }

  return agreement.toPrecision(5);
}

/**
 * saveReport
 */

function saveReport(row) {
  const key = [
    smoment(row.date).hbaseFormatStartRow(),
    row.validator
  ].join('|');

  return hbase.putRow('validator_reports', key, row);
}

/**
 * saveReports
 * save data to hbase
 */

function saveReports() {
  let report;
  let key;
  let date;

  for (key in validators) {
    for (date in validators[key]) {
      report = validators[key][date];
      report.main_net_agreement =
        caclulateAgreement(report.main_net_ledgers, clusterLedgers.main[date]);
      report.alt_net_agreement =
        caclulateAgreement(report.alt_net_ledgers, clusterLedgers.alt[date]);
      saveReport(report);
    }
  }
}

/**
 * purge
 * purge cached data
 */

function purge() {
  const now = smoment();
  const today = smoment();
  const maxTime = 120 * 1000;
  let key;
  let date;

  today.moment.startOf('day');

  for (key in ledgers) {
    if (now.moment.diff(ledgers[key].timestamp) > maxTime) {
      updateReports(ledgers[key]);
      delete ledgers[key];
    }
  }

  for (key in validations) {
    if (now.moment.diff(validations[key].timestamp) > maxTime) {
      delete validations[key];
    }
  }

  for (key in validators) {
    for (date in validators[key]) {
      if (today.moment.diff(date)) {
        console.log('deleting', key, date, now.format());
        delete validators[key][date];
      }
    }
  }

  for (key in clusterLedgers) {
    for (date in clusterLedgers[key]) {
      if (today.moment.diff(date)) {
        console.log('deleting', key, date, now.format());
        delete clusterLedgers[key][date];
      }
    }
  }

  console.log('ledgers:', Object.keys(ledgers).length);
  saveReports();
}

/**
 * loadReports
 * load data from hbase
 */

function loadReports() {
  const date = smoment();
  date.moment.startOf('day');
  date.granularity = 'day';

  // get cluster ledgers
  hbase.getScan({
    table: 'cluster_ledgers',
    startRow: date.hbaseFormatStartRow(),
    stopRow: date.hbaseFormatStopRow()
  }, function(err, resp) {
    if (resp) {
      resp.forEach(c => {
        clusterLedgers[c.cluster][date.format()] = Number(c.count);
      });

    } else if (err) {
      console.log(err);
    }

    console.log(clusterLedgers);
  });

  // get validator reports
  hbase.getScan({
    table: 'validator_reports',
    startRow: date.hbaseFormatStartRow(),
    stopRow: date.hbaseFormatStopRow()
  }, function(err, resp) {
    if (resp) {
      console.log('Validators:', resp.length);
      resp.forEach(v => {
        validators[v.validator] = {};
        validators[v.validator][v.date] = {
          validator: v.validator,
          date: v.date,
          total_ledgers: Number(v.total_ledgers),
          main_net_ledgers: Number(v.main_net_ledgers),
          alt_net_ledgers: Number(v.alt_net_ledgers),
          other_ledgers: Number(v.other_ledgers)
        };
      });

    } else if (err) {
      console.log(err);
    }
  });
}

/**
 * start
 * set purge interval
 * and load historical
 * data
 */

function start() {
  setInterval(purge, 10000);
  loadReports();
}

module.exports = {
  handleValidation: handleValidation,
  start: start
};
