var config = require('./config');
var assert = require('assert');
var request = require('request');
var Promise = require('bluebird')
const Hbase = require('../lib/hbase/hbase-client');;
var smoment = require('../lib/smoment');
var Manifests = require('../lib/validations/manifests');
const dateFormat = 'YYYY-MM-DDTHH:mm:ss.SSS[Z]';

var hbaseConfig = config.get('hbase');
var port = config.get('port') || 7111;
var prefix = config.get('prefix') || 'TEST_';

hbaseConfig.prefix = prefix;

const hbase = new Hbase(hbaseConfig);

var manifests;

beforeEach(function(done) {
  manifests = new Manifests(hbaseConfig);
  hbase.deleteAllRows({
    table: 'manifests_by_master_key'
  }).then(() => {
    hbase.deleteAllRows({
      table: 'manifests_by_validator'
    }).then(() => { done(); })
  });
});

describe('manifests', function(done) {
  it('should save manifests into hbase', function(done) {
    const manifest = {
      signing_key: 'n9LRZXPh1XZaJr5kVpdciN76WCCcb5ZRwjvHywd4Vc4fxyfGEDJA',
      master_key: 'nHU5wPBpv1kk3kafS2ML2GhyoGJuHhPP4fCa2dwYUjMT5wR8Dk5B',
      seq: 4,
      signature: 'ba37041d4d9739ebf721a75f7a9e408d92b9920e71a6af5a9fe11e88f05c8937771e1811cf262f489b69c67cc80c96518a6e5c17091dd743246229d21ee2c00c'
    }

    manifests.handleManifest(manifest)
    .then(() => {
      return hbase.getAllRows({
        table: 'manifests_by_validator'
      });
    }).then((rows) => {
      assert.strictEqual(rows.length, 1);
      assert.strictEqual(rows[0].master_public_key, manifest.master_key);
      assert.strictEqual(rows[0].ephemeral_public_key, manifest.signing_key);
      assert.strictEqual(rows[0].sequence, manifest.seq.toString());
      assert.strictEqual(rows[0].signature, manifest.signature);
      return hbase.getAllRows({
        table: 'manifests_by_master_key'
      })
    }).then((rows) => {
      assert.strictEqual(rows.length, 1);
      assert.strictEqual(rows[0].ephemeral_public_key, manifest.signing_key);
      assert.strictEqual(rows[0].sequence, manifest.seq.toString());
      done();
    });
  });

  it('should require an ephemeral key', function(done) {
    manifests.handleManifest({
      master_key: 'nHU5wPBpv1kk3kafS2ML2GhyoGJuHhPP4fCa2dwYUjMT5wR8Dk5B',
      seq: 4,
      signature: 'ba37041d4d9739ebf721a75f7a9e408d92b9920e71a6af5a9fe11e88f05c8937771e1811cf262f489b69c67cc80c96518a6e5c17091dd743246229d21ee2c00c'
    }).catch((err) => {
      assert.strictEqual(err, 'signing_key cannot be null');
      done();
    });
  });

  it('should require a master key', function(done) {
    manifests.handleManifest({
      signing_key: 'n9LRZXPh1XZaJr5kVpdciN76WCCcb5ZRwjvHywd4Vc4fxyfGEDJA',
      seq: 4,
      signature: 'ba37041d4d9739ebf721a75f7a9e408d92b9920e71a6af5a9fe11e88f05c8937771e1811cf262f489b69c67cc80c96518a6e5c17091dd743246229d21ee2c00c'
    }).catch((err) => {
      assert.strictEqual(err, 'master_key cannot be null');
      done();
    });
  });

  it('should require a sequence', function(done) {
    manifests.handleManifest({
      master_key: 'nHU5wPBpv1kk3kafS2ML2GhyoGJuHhPP4fCa2dwYUjMT5wR8Dk5B',
      signing_key: 'n9LRZXPh1XZaJr5kVpdciN76WCCcb5ZRwjvHywd4Vc4fxyfGEDJA',
      signature: 'ba37041d4d9739ebf721a75f7a9e408d92b9920e71a6af5a9fe11e88f05c8937771e1811cf262f489b69c67cc80c96518a6e5c17091dd743246229d21ee2c00c'
    }).catch((err) => {
      assert.strictEqual(err, 'seq cannot be null');
      done();
    });
  });

  it('should require a signature', function(done) {
    manifests.handleManifest({
      master_key: 'nHU5wPBpv1kk3kafS2ML2GhyoGJuHhPP4fCa2dwYUjMT5wR8Dk5B',
      signing_key: 'n9LRZXPh1XZaJr5kVpdciN76WCCcb5ZRwjvHywd4Vc4fxyfGEDJA',
      seq: 4,
    }).catch((err) => {
      assert.strictEqual(err, 'signature cannot be null');
      done();
    });
  });

  it('should require a valid signature', function(done) {
    manifests.handleManifest({
      master_key: 'nHU5wPBpv1kk3kafS2ML2GhyoGJuHhPP4fCa2dwYUjMT5wR8Dk5B',
      signing_key: 'n9LRZXPh1XZaJr5kVpdciN76WCCcb5ZRwjvHywd4Vc4fxyfGEDJA',
      seq: 4,
      signature: 'badsig'
    }).catch((err) => {
      assert.strictEqual(err, 'Manifest has invalid signature');
      done();
    });
  });

  it('should cache new manifests', function(done) {
    const master_public_key = 'nHU5wPBpv1kk3kafS2ML2GhyoGJuHhPP4fCa2dwYUjMT5wR8Dk5B'
    const old_ephemeral_public_key = 'n9KXuFUqkykLVr8oDwDeNuu33akSuUXShNER4y96Uco88R4xwpB5'
    const new_ephemeral_public_key = 'n9LRZXPh1XZaJr5kVpdciN76WCCcb5ZRwjvHywd4Vc4fxyfGEDJA'

    manifests.handleManifest({
      signing_key: old_ephemeral_public_key,
      master_key: master_public_key,
      seq: 2,
      signature: '58a01747386a7dc26e21512c52a7a01ef6ad2efc99fc2ecf0d288665f7bf7e831949abf7129dada2c47f5633ffa73a1a00d5fc061892ecead3a014a99924480e'
    }).then(() => {
      return manifests.handleManifest({
        signing_key: new_ephemeral_public_key,
        master_key: master_public_key,
        seq: 4,
        signature: 'ba37041d4d9739ebf721a75f7a9e408d92b9920e71a6af5a9fe11e88f05c8937771e1811cf262f489b69c67cc80c96518a6e5c17091dd743246229d21ee2c00c'
      });
    }).then(() => {
      return hbase.getAllRows({
        table: 'manifests_by_validator'
      });
    }).then((rows) => {
      assert.strictEqual(rows.length, 2);
      assert.strictEqual(rows[0].master_public_key, master_public_key);
      assert.strictEqual(rows[0].ephemeral_public_key, old_ephemeral_public_key);
      assert.strictEqual(rows[1].master_public_key, master_public_key);
      assert.strictEqual(rows[1].ephemeral_public_key, new_ephemeral_public_key);
      return hbase.getAllRows({
        table: 'manifests_by_master_key'
      });
    }).then((rows) => {
      assert.strictEqual(rows.length, 1);
      assert.strictEqual(rows[0].ephemeral_public_key, new_ephemeral_public_key);
      assert.strictEqual(rows[0].sequence, '4');
      done();
    });
  });

  it('should not cache new stale manifests', function(done) {
    const master_public_key = 'nHU5wPBpv1kk3kafS2ML2GhyoGJuHhPP4fCa2dwYUjMT5wR8Dk5B'
    const old_ephemeral_public_key = 'n9LRZXPh1XZaJr5kVpdciN76WCCcb5ZRwjvHywd4Vc4fxyfGEDJA'
    const new_ephemeral_public_key = 'n9KXuFUqkykLVr8oDwDeNuu33akSuUXShNER4y96Uco88R4xwpB5'

    manifests.handleManifest({
      signing_key: old_ephemeral_public_key,
      master_key: master_public_key,
      seq: 4,
      signature: 'ba37041d4d9739ebf721a75f7a9e408d92b9920e71a6af5a9fe11e88f05c8937771e1811cf262f489b69c67cc80c96518a6e5c17091dd743246229d21ee2c00c'
    }).then(() => {
      return manifests.handleManifest({
        signing_key: new_ephemeral_public_key,
        master_key: master_public_key,
        seq: 2,
        signature: '58a01747386a7dc26e21512c52a7a01ef6ad2efc99fc2ecf0d288665f7bf7e831949abf7129dada2c47f5633ffa73a1a00d5fc061892ecead3a014a99924480e'
      });
    }).then(() => {
      return hbase.getAllRows({
        table: 'manifests_by_validator'
      });
    }).then((rows) => {
      assert.strictEqual(rows.length, 2);
      assert.strictEqual(rows[0].master_public_key, master_public_key);
      assert.strictEqual(rows[0].ephemeral_public_key, new_ephemeral_public_key);
      assert.strictEqual(rows[1].master_public_key, master_public_key);
      assert.strictEqual(rows[1].ephemeral_public_key, old_ephemeral_public_key);
      return hbase.getAllRows({
        table: 'manifests_by_master_key'
      });
    }).then((rows) => {
      assert.strictEqual(rows.length, 1);
      assert.strictEqual(rows[0].ephemeral_public_key, old_ephemeral_public_key);
      assert.strictEqual(rows[0].sequence, '4');
      done();
    });
  });

  it('should not cache when first manifest revokes the master key', function(done) {

    manifests.handleManifest({
      signing_key: 'n9KVoK1g4NuSuMXqScpVhRVbBDNmLD8tPWeRKdewNVUN5F87YjwR',
      master_key: 'nHU5wPBpv1kk3kafS2ML2GhyoGJuHhPP4fCa2dwYUjMT5wR8Dk5B',
      seq: 4294967295,
      signature: '4d62df5d4cdc66b96a2fab58739515a636662cdf9d50d3b35ca7986293a72c3c0b5d518355274850e73e801628c5b6c7830546eab098f4e4548199bdc4993405'
    }).then(() => {
      return hbase.getAllRows({
        table: 'manifests_by_validator'
      });
    }).then((rows) => {
      assert.strictEqual(rows.length, 1);
      return hbase.getAllRows({
        table: 'manifests_by_master_key'
      });
    }).then((rows) => {
      assert.strictEqual(rows.length, 0);
      done();
    });
  });

  it('should remove master key from cache when revoked', function(done) {

    manifests.handleManifest({
      signing_key: 'n9LRZXPh1XZaJr5kVpdciN76WCCcb5ZRwjvHywd4Vc4fxyfGEDJA',
      master_key: 'nHU5wPBpv1kk3kafS2ML2GhyoGJuHhPP4fCa2dwYUjMT5wR8Dk5B',
      seq: 4,
      signature: 'ba37041d4d9739ebf721a75f7a9e408d92b9920e71a6af5a9fe11e88f05c8937771e1811cf262f489b69c67cc80c96518a6e5c17091dd743246229d21ee2c00c'
    }).then(() => {
      return manifests.handleManifest({
        signing_key: 'n9KVoK1g4NuSuMXqScpVhRVbBDNmLD8tPWeRKdewNVUN5F87YjwR',
        master_key: 'nHU5wPBpv1kk3kafS2ML2GhyoGJuHhPP4fCa2dwYUjMT5wR8Dk5B',
        seq: 4294967295,
        signature: '4d62df5d4cdc66b96a2fab58739515a636662cdf9d50d3b35ca7986293a72c3c0b5d518355274850e73e801628c5b6c7830546eab098f4e4548199bdc4993405'
      });
    }).then(() => {
      return hbase.getAllRows({
        table: 'manifests_by_validator'
      });
    }).then((rows) => {
      assert.strictEqual(rows.length, 2);
      return hbase.getAllRows({
        table: 'manifests_by_master_key'
      });
    }).then((rows) => {
      assert.strictEqual(rows.length, 0);
      done();
    });
  });

  it('should return cached master key', function(done) {
    const manifest = {
      signing_key: 'n9LRZXPh1XZaJr5kVpdciN76WCCcb5ZRwjvHywd4Vc4fxyfGEDJA',
      master_key: 'nHU5wPBpv1kk3kafS2ML2GhyoGJuHhPP4fCa2dwYUjMT5wR8Dk5B',
      seq: 4,
      signature: 'ba37041d4d9739ebf721a75f7a9e408d92b9920e71a6af5a9fe11e88f05c8937771e1811cf262f489b69c67cc80c96518a6e5c17091dd743246229d21ee2c00c'
    }

    manifests.handleManifest(manifest)
    .then(() => {
      const master_public_key = manifests.getMasterKey(manifest.ephemeral_public_key);
      assert.strictEqual(master_public_key, manifest.master_public_key);
      done();
    });
  })

  it('should store manifests from database in cache', function(done) {
    const manifest = {
      master_public_key: 'n9LRZXPh1XZaJr5kVpdciN76WCCcb5ZRwjvHywd4Vc4fxyfGEDJA',
      ephemeral_public_key: 'nHU5wPBpv1kk3kafS2ML2GhyoGJuHhPP4fCa2dwYUjMT5wR8Dk5B',
      sequence: 4
    };

    const row = {
      table: 'manifests_by_master_key',
      rowkey: manifest.master_public_key,
      columns: {
        ephemeral_public_key: manifest.ephemeral_public_key,
        sequence: manifest.sequence
      }
    };

    hbase.putRow(row)
    .then(() => {
      return hbase.getAllRows({
        table: 'manifests_by_master_key'
      });
    }).then((rows) => {
      assert.strictEqual(rows.length, 1);
      var master_public_key = manifests.getMasterKey(manifest.ephemeral_public_key);
      assert.strictEqual(master_public_key, undefined);
      return manifests.start();
    }).then(() => {
      master_public_key = manifests.getMasterKey(manifest.ephemeral_public_key);
      assert.strictEqual(master_public_key, manifest.master_public_key);
      done();
    }).catch((e) => {
      assert.ifError(e);
    });
  });
});
