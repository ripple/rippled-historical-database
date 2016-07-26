'use strict';

const dateFormat = 'YYYY-MM-DDTHH:mm:ss.SSS[Z]';
const Promise = require('bluebird');
const addressCodec = require('ripple-address-codec');
const CronJob = require('cron').CronJob;
const elliptic = require('elliptic');
const hash = require('hash.js');
const Hbase = require('../hbase/hbase-client');
const Logger = require('../logger');
const log = new Logger({scope: 'validations'});
const smoment = require('../smoment');
const Verifier = require('ripple-domain-verifier');
const verifier = new Verifier();
const curve = elliptic.curves['secp256k1'];
const ecdsa = new elliptic.ec(curve);


// types (common)
const STI_UINT32 = 2
const STI_UINT64 = 3
const STI_HASH256 = 5
const STI_VL = 7

// types (uncommon)
const STI_VECTOR256 = 19

function field_code (sti, name) {
  var data = []
  if (sti < 16) {
    if (name < 16) {
      data.push((sti << 4) | name)
    } else {
      data.push((sti << 4), name)
    }
  } else if (name < 16) {
      data.push(name, sti)
  } else {
    data.push(0, sti, name)
  }
  return data
}

const sfFlags            = field_code(STI_UINT32,  2)   // required
const sfLedgerSequence   = field_code(STI_UINT32,  6)
const sfCloseTime        = field_code(STI_UINT32,  7)
const sfSigningTime      = field_code(STI_UINT32,  9)   // required
const sfLoadFee          = field_code(STI_UINT32, 24)
const sfReserveBase      = field_code(STI_UINT32, 31)
const sfReserveIncrement = field_code(STI_UINT32, 32)
const sfBaseFee          = field_code(STI_UINT64,  5)
const sfLedgerHash       = field_code(STI_HASH256, 1)   // required
const sfSigningPubKey    = field_code(STI_VL, 3)        // required
const sfSignature        = field_code(STI_VL, 6)
const sfAmendments       = field_code(STI_VECTOR256, 3)

function addUInt32(s, field_id, value) {
  if (!value) throw new Error('missing addUInt32 field value')
  var buf = new Buffer(4)
  buf.writeUInt32BE(value)
  return s.concat(new Buffer(field_id).toJSON().data,
                  buf.toJSON().data)
}

/**
 * Validations
 */

const Validations = function(config) {

  config.logLevel = 2;

  const hbase = new Hbase(config);
  const reports = require('./reports')(hbase);
  const validations = {};

  function verifySignature(validation) {

    // Serialize and hash the validation
    // Fields are added in order of: (sti << 16) | name
    var val = new Buffer('VAL\0').toJSON().data

    val = addUInt32(val, sfFlags, validation.flags)
    if (validation.ledger_index) {
      val = addUInt32(val, sfLedgerSequence, validation.ledger_index)
    }
    val = addUInt32(val, sfSigningTime, validation.signing_time)
    if (validation.load_fee) {
      val = addUInt32(val, sfLoadFee, validation.load_fee)
    }
    if (validation.reserve_base) {
      val = addUInt32(val, sfReserveBase, validation.reserve_base)
    }
    if (validation.reserve_inc) {
      val = addUInt32(val, sfReserveIncrement, validation.reserve_inc)
    }
    if (validation.base_fee) {
      var base_fee_hex = validation.base_fee.toString(16)
      while (base_fee_hex.length<16) {
        base_fee_hex = '0' + base_fee_hex
      }
      val = val.concat(new Buffer(sfBaseFee).toJSON().data,
                       new Buffer(base_fee_hex, 'hex').toJSON().data)
    }
    val = val.concat(new Buffer(sfLedgerHash).toJSON().data,
                     new Buffer(validation.ledger_hash, 'hex').toJSON().data)
    const signing_pub_key_bytes = addressCodec.decodeNodePublic(
                                    validation.ephemeral_public_key ?
                                    validation.ephemeral_public_key :
                                    validation.validation_public_key)
    val = val.concat(new Buffer(sfSigningPubKey).toJSON().data,
                     [signing_pub_key_bytes.length],
                     signing_pub_key_bytes)
    if (validation.amendments) {
      var amendments_bytes = []
      for (var amendment of validation.amendments) {
        amendments_bytes = amendments_bytes.concat(
                             new Buffer(amendment, 'hex').toJSON().data)
      }
      val = val.concat(new Buffer(sfAmendments).toJSON().data,
                     [validation.amendments.length * 256 / 8],
                     amendments_bytes)
    }
    var val_hash = hash.sha512().update(val).digest("hex").substring(0, 64)

    return ecdsa.verify(val_hash,
                        new Buffer(validation.signature, 'hex').toJSON().data,
                        signing_pub_key_bytes)
  }

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

              if (!verifySignature(validation)) {
                return reject('invalid signature')
              }
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
