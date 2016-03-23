'use strict';

const dateFormat = 'YYYY-MM-DDTHH:mm:ss.SSS[Z]';
const Promise = require('bluebird');
const addressCodec = require('ripple-address-codec');
const elliptic = require('elliptic');
const Ed25519 = elliptic.eddsa('ed25519');
const smoment = require('../smoment');
const Hbase = require('../hbase/hbase-client');
const Logger = require('../logger');
const log = new Logger({scope : 'manifests'});

/**
 * Manifests
 */

const Manifests = function(config) {

  config.logLevel = 2;

  const hbase = new Hbase(config);
  Promise.promisifyAll(hbase);

  const MAX_SEQUENCE = 4294967295

  // Store a map of ephemeral to master public keys for quick lookup
  // to check each incoming validation for a known master public key.
  var manifests = {};

  var master_keys = {};

  function clearCache() {
    master_keys = {};
  }

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
    const ephemeral_public_bytes = addressCodec.decodeNodePublic(manifest.ephemeral_public_key)
    const signature_bytes = new Buffer(manifest.signature, 'hex').toJSON().data

    var manifest_data = new Buffer('MAN\0').toJSON().data
    manifest_data = manifest_data.concat(new Buffer(sfSequence).toJSON().data,
                               sequence_bytes,
                               new Buffer(sfPublicKey).toJSON().data,
                               [master_public_bytes.length],
                               master_public_bytes,
                               new Buffer(sfSigningPubKey).toJSON().data,
                               [ephemeral_public_bytes.length],
                               ephemeral_public_bytes)

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

  function saveManifest(manifest) {
    const key = [
      manifest.master_public_key,
      manifest.sequence
    ].join('|');

    const row = {
      table: 'manifests_by_validator',
      rowkey: key,
      columns: {
        master_public_key: manifest.master_public_key,
        ephemeral_public_key: manifest.ephemeral_public_key,
        sequence: manifest.sequence,
        signature: manifest.signature,
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
    }, function(err, manifest) {
      if (!manifest) {
        return Promise.resolve();
      }

      if (manifests.ephemeral_public_key) {
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
    hbase.putRow('manifests_by_validator', key, {
      last_datetime: manifest.last_datetime,
      count: manifests.count
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
      if (now.moment.diff(manifests[key].timestamp) > maxTime) {
        delete manifests[key];
      }
    }

    count = Object.keys(manifests).length;
    console.log(('cached manifests: ' + count).green);
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
        setInterval(function() {
          console.log('hbase connections:', hbase.pool.length);
        }, 60 * 1000);
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
        date: smoment(),
        timestamp: smoment().format(dateFormat),
        count: 1
      };

      const key = [
        manifest.master_public_key,
        manifest.sequence
      ].join('|');

      return new Promise((resolve, reject) => {
        if (!manifest.master_public_key) {
          return reject('master_key cannot be null');
        } else if (!manifest.ephemeral_public_key) {
          return reject('signing_key cannot be null');
        } else if (!manifest.sequence) {
          return reject('seq cannot be null');
        } else if (!manifest.signature) {
          return reject('signature cannot be null');
        }

        // already encountered
        if (manifests[key]) {
          manifests[key].count++;
          manifests[key].last_datetime = manifest.timestamp;

          clearTimeout(manifests[key].debounce);
          manifests[key].debounce = setTimeout(updateManifest.bind(this),
                                               1000, key, manifests[key]);

        // first encounter
        } else {

          // Check signature
          if (!verifySignature(manifest)) {
            return reject('Manifest has invalid signature');
          }

          return saveManifest(manifest).then(() => {
            // Check if this revokes previous ephemeral keys or the master key itself
            if (manifest.sequence>=MAX_SEQUENCE) {
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
                setActiveManifest(manifest);
              } else if (active_manifest.sequence<manifest.sequence) {

                // Update cache
                setActiveManifest(manifest);
                deleteKey(active_manifest.ephemeral_public_key)
              }

              resolve();
            });
          });
        }
      });
    }
  }
};

module.exports = Manifests
