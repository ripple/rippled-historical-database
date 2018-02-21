'use strict';

const smoment = require('../smoment');
const Promise = require('bluebird');
const Logger = require('../logger');
const log = new Logger({scope : 'validator reports'});
const colors = require('colors');
const hbase = require('../hbase');
const request = require('request-promise');
const addressCodec = require('ripple-address-codec');
const binary = require('ripple-binary-codec');
const elliptic = require('elliptic');
const Ed25519 = elliptic.eddsa('ed25519');

let clusters = {
  alt: {
    pubkey: 'ED264807102805220DA0F312E71FC2C69E1552C9C5790F6C25E3729DEB573D5860',
    site: 'https://vl.altnet.rippletest.net',
    validators: [],
    quorum: 0
  },
  main: {
    pubkey: 'ED2677ABFFD1B33AC6FBC3062B71F1E8397C1505E1C42C64D11AD1B28FF73F4734',
    site: 'https://vl.ripple.com',
    validators: [],
    quorum: 0
  }
};

function parseManifest (data) {
  let man_data = new Buffer(data, 'base64');
  let manhex = man_data.toString('hex').toUpperCase();
  return binary.decode(manhex)
}

function toBytes(hex) {
  return new Buffer(hex, 'hex').toJSON().data;
}

function hextoBase58 (hex) {
  return addressCodec.encodeNodePublic(toBytes(hex))
}

function verifyManifest(manifest) {
  const signature = manifest.MasterSignature;
  delete manifest.Signature;
  delete manifest.MasterSignature;

  const manifest_data = new Buffer('MAN\0').toJSON().data.concat(toBytes(binary.encode(manifest)));

  let master_public_bytes = toBytes(manifest.PublicKey);
  master_public_bytes.shift();

  return Ed25519.verify(manifest_data, toBytes(signature), master_public_bytes);
}

function getUNLs () {
  return Promise.map(Object.keys(clusters), name => {
    return request.get({
      url: clusters[name].site,
      json: true
    }).then(data => {
      let manifest = parseManifest(data.manifest);
      if (!verifyManifest(manifest)) {
        log.error('invalid manifest signature on', clusters[name].site);
        return Promise.resolve();
      }

      let buff = new Buffer(data.blob, 'base64');
      let pubkey_bytes = toBytes(manifest.SigningPubKey);
      pubkey_bytes.shift();

      if (!Ed25519.verify(buff, toBytes(data.signature), pubkey_bytes)) {
        log.error('invalid signature on', clusters[name].site);
        return Promise.resolve();
      }

      const valList = JSON.parse(buff.toString('ascii'));
      clusters[name].validators = [];
      for (const validator of valList.validators) {
        clusters[name].validators.push(hextoBase58(validator.validation_public_key))
      }

      const nVals = clusters[name].validators.length;
      clusters[name].quorum = (nVals <= 6) ?
        (Math.floor(nVals/2) + 1) : (Math.floor(nVals * 2/3) + 1);
      return Promise.resolve();
    });
  });
}

module.exports = function() {

  /**
   * isClusterLedger
   */

  function isClusterLedger(name, ledger) {
    let count = 0;

    for (let pubkey of clusters[name].validators) {
      if (ledger.validators[pubkey]) {
        count++;
        if (count >= clusters[name].quorum) {
          return true;
        }
      }
    }

    return false;
  }

  /**
   * calculateAgreement
   */

  function calculateAgreement(count, clusterCount) {
    let agreement = 0;

    if (clusterCount) {
      agreement = count / clusterCount;
    }

    return agreement.toFixed(5);
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
          log.info('validations found:', validations.length.toString().underline);
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
        calculateAgreement(validator.main_net_ledgers, totalMainLedgers);
      validator.alt_net_agreement =
        calculateAgreement(validator.alt_net_ledgers, totalAltLedgers);
    }

    log.info('main net ledgers:'.cyan, totalMainLedgers.toString().cyan.underline);
    log.info('alt net ledgers:'.cyan, totalAltLedgers.toString().cyan.underline);
    log.info('total ledgers:'.cyan, Object.keys(data.ledgers).length.toString().cyan.underline);
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
      return getUNLs().then(() => {
        if (!clusters['main'].validators.length) {
          log.error('Unable to determine trusted validators. Skipping report generation.');
          return Promise.resolve();
        }
        return getValidations(start, end)
        .then(processValidations.bind(this, start))
        .then(createReports)
        .then(saveReports);
      })
    }
  };
};
