'use strict';

const dateFormat = 'YYYY-MM-DDTHH:mm:ss.SSS[Z]';
const Promise = require('bluebird');
const addressCodec = require('ripple-address-codec');
const elliptic = require('elliptic');
const Ed25519 = elliptic.eddsa('ed25519');
const smoment = require('../smoment');
const hbase = require('../hbase');
const Logger = require('../logger');
const log = new Logger({scope : 'manifests'});
const utils = require('../utils');
Promise.promisifyAll(hbase);

/**
 * Manifests
 */

const Manifests = function() {

  const MAX_SEQUENCE = 4294967295
  const SEQ_PAD = MAX_SEQUENCE.toString().length

  // Store a map of ephemeral to master public keys for quick lookup
  // to check each incoming validation for a known master public key.
  var master_keys = {};

  // cache
  var manifests = {};

  function deleteKey(ephemeral_public_key) {
    delete master_keys[ephemeral_public_key];
  }

  function setKey(ephemeral_public_key, master_public_key) {
    master_keys[ephemeral_public_key] = master_public_key;
  }

  function verifySignature(manifest) {
    const sfSequence = '$'
    const sfPublicKey = 'q'
    const sfSigningPubKey = 's'

    // Form manifest
    var sequence_buf = new Buffer(4)
    sequence_buf.writeUInt32BE(manifest.sequence)
    const sequence_bytes = sequence_buf.toJSON().data

    var master_public_bytes = addressCodec.decodeNodePublic(manifest.master_public_key)
    const signature = manifest.master_signature ? manifest.master_signature : manifest.signature
    const signature_bytes = new Buffer(signature, 'hex').toJSON().data

    var manifest_data = new Buffer('MAN\0').toJSON().data
    manifest_data = manifest_data.concat(new Buffer(sfSequence).toJSON().data,
                               sequence_bytes,
                               new Buffer(sfPublicKey).toJSON().data,
                               [master_public_bytes.length],
                               master_public_bytes)

    if (manifest.ephemeral_public_key) {
      const ephemeral_public_bytes = addressCodec.decodeNodePublic(manifest.ephemeral_public_key)
      manifest_data = manifest_data.concat(
        new Buffer(sfSigningPubKey).toJSON().data,
        [ephemeral_public_bytes.length],
        ephemeral_public_bytes)
    }

    master_public_bytes.shift()
    if (!Ed25519.verify(manifest_data, signature_bytes, master_public_bytes)) {
      return false;
    }
    return true;
  };

  /**
   * saveManifest
   * save data to hbase
   */

  function saveManifest(key, manifest) {
    const row = {
      table: 'manifests_by_validator',
      rowkey: key,
      columns: {
        master_public_key: manifest.master_public_key,
        ephemeral_public_key: manifest.ephemeral_public_key,
        sequence: manifest.sequence,
        signature: manifest.signature,
        master_signature: manifest.master_signature,
        first_datetime: manifest.timestamp,
        last_datetime: manifest.timestamp,
        count: manifest.count
      }
    };
    return hbase.putRow(row);
  }

  /**
   * setActiveManifest
   */
  function setActiveManifest(manifest) {
    setKey(manifest.ephemeral_public_key, manifest.master_public_key);

    return hbase.putRow({
      table: 'manifests_by_master_key',
      rowkey: manifest.master_public_key,
      columns: {
        ephemeral_public_key: manifest.ephemeral_public_key,
        sequence: manifest.sequence
      }
    });
  }

  /**
   * deleteActiveManifest
   * master key has been revoked
   */
  function deleteActiveManifest(master_public_key) {
    return hbase.getRowAsync({
      table: 'manifests_by_master_key',
      rowkey: master_public_key,
    }).then(manifest => {
      if (!manifest) {
        return Promise.resolve();
      }

      if (manifest.ephemeral_public_key) {
        deleteKey(manifest.ephemeral_public_key);
      }

      return hbase.deleteRow({
        table: 'manifests_by_master_key',
        rowkey: manifest.rowkey
      });
    });
  }

  /**
   * updateManifest
   */

  function updateManifest(key, manifest) {
    return hbase.putRow({
      table: 'manifests_by_validator',
      rowkey: key,
      columns: {
        last_datetime: manifest.last_datetime,
        count: manifest.count
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

    for (key in manifests) {
      if (now.moment.diff(manifests[key].last_datetime) > maxTime) {
        delete manifests[key];
      }
    }

    count = Object.keys(manifests).length;
    log.info(('cached manifests: ' + count).green);
  }

  function getCount(key) {
    if (manifests[key]) {
      return Promise.resolve(manifests[key].count);
    }

    return hbase.getRowAsync({
      table: 'manifests_by_validator',
      rowkey: key,
    }).then(manifest => {
      if (manifest) {
        return Promise.resolve(Number(manifest.count));
      } else {
        return Promise.resolve(0);
      }
    });
  }

  /**
   * start
   * set purge interval
   * and load historical
   * data
   */

  return {
    start: function() {

      return hbase.getAllRows({
        table: 'manifests_by_master_key'
      }).then((rows) => {
        for (const row of rows) {
          setKey(row.ephemeral_public_key, row.rowkey);
        }

        setInterval(purge, 30 * 1000);
      });
    },

    getMasterKey: function(ephemeral_public_key) {
      return master_keys[ephemeral_public_key]
    },

    /**
     * handleManifest
     */

    handleManifest: function(data) {
      const manifest = {
        master_public_key: data.master_key,
        ephemeral_public_key: data.signing_key,
        sequence: data.seq,
        signature: data.signature,
        master_signature: data.master_signature,
        timestamp: smoment().format(dateFormat),
        count: 1
      };

      return new Promise((resolve, reject) => {
        if (!manifest.master_public_key) {
          return reject('master_key cannot be null');
        } else if (!manifest.sequence) {
          return reject('seq cannot be null');
        } else if (manifest.sequence > MAX_SEQUENCE) {
          return reject('seq cannot be greater than MAX_SEQUENCE');
        } else if (!manifest.ephemeral_public_key &&
                    manifest.sequence !== MAX_SEQUENCE) {
          return reject('signing_key cannot be null');
        } else if (!manifest.master_signature && !manifest.signature) {
          return reject('master signature and signature cannot be null');
        }

        const key = [
          manifest.master_public_key,
          utils.padNumber(manifest.sequence, SEQ_PAD),
          manifest.ephemeral_public_key
        ].join('|');

        getCount(key)
        .then(count => {

          // already encountered
          if (count) {
            if (manifests[key] && manifests[key].debounce) {
              clearTimeout(manifests[key].debounce);
            }

            manifests[key] = {
              count: count + 1,
              last_datetime: manifest.timestamp
            }

            manifests[key].debounce = setTimeout(updateManifest.bind(this),
                                                 1000, key, manifests[key])

            return resolve();
          }

          // first encounter

          // Check signature
          if (!verifySignature(manifest)) {
            return reject('Manifest has invalid signature');
          }

          manifests[key] = {
            count: 1,
            last_datetime: manifest.timestamp
          }

          return saveManifest(key, manifest).then(() => {

            // Check if this revokes previous ephemeral keys or the master key itself
            if (manifest.sequence==MAX_SEQUENCE) {
              deleteActiveManifest(manifest.master_public_key)
              return resolve();
            }

            // Lookup manifest for this master_public_key with highest sequence
            return hbase.getRowAsync({
              table: 'manifests_by_master_key',
              rowkey: manifest.master_public_key
            }).then((active_manifest) => {

              if (!active_manifest) {

                // Add new master key to cache
                return setActiveManifest(manifest)
                .then(() => {
                  resolve();
                });
              } else if (active_manifest.sequence<manifest.sequence) {

                // Update cache
                return setActiveManifest(manifest)
                .then(() => {
                  deleteKey(active_manifest.ephemeral_public_key)
                  resolve();
                });
              }

              resolve();
            });
          });
        });
      });
    }
  }
};

module.exports = Manifests
