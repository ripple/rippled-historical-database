var config = require('../config');
var assert = require('assert');
var request = require('request');
var Promise = require('bluebird');
var hbase = require('../lib/hbase');
var smoment = require('../lib/smoment');
var mockManifests = require('./mock/manifests.json')
var mockResponses = require('./mock/manifests.responses.json')
var Manifests = require('../lib/validations/manifests');
var _ = require('underscore');
var dateFormat = 'YYYY-MM-DDTHH:mm:ss.SSS[Z]';
var manifests;
var port = config.get('port') || 7111

function makeRowKey(master_public_key, sequence, ephemeral_public_key) {
  return [
    master_public_key,
    sequence.toString().padStart(10, '0'),
    ephemeral_public_key
  ].join('|');
}

describe('handleManifest', function(done) {
  beforeEach(function(done) {
    manifests = new Manifests();
    hbase.deleteAllRows({
      table: 'manifests_by_master_key'
    }).then(() => {
      hbase.deleteAllRows({
        table: 'manifests_by_validator'
      }).then(() => { done(); })
    });
  });

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
      assert.strictEqual(rows[0].rowkey, makeRowKey(manifest.master_key, manifest.seq, manifest.signing_key));
      assert.strictEqual(rows[0].master_public_key, manifest.master_key);
      assert.strictEqual(rows[0].ephemeral_public_key, manifest.signing_key);
      assert.strictEqual(rows[0].sequence, manifest.seq.toString());
      assert.strictEqual(rows[0].signature, manifest.signature);
      return hbase.getAllRows({
        table: 'manifests_by_master_key'
      })
    }).then((rows) => {
      assert.strictEqual(rows.length, 1);
      assert.strictEqual(rows[0].rowkey, manifest.master_key);
      assert.strictEqual(rows[0].ephemeral_public_key, manifest.signing_key);
      assert.strictEqual(rows[0].sequence, manifest.seq.toString());
      done();
    });
  });

  it('should accept second signature', function(done) {
    manifests.handleManifest({
      master_key: 'nHUM1j7YGDVH7VbYw7bvjh9QR4f59GmQjwaNvnG34ki6U2upPYmY',
      signing_key: 'n9Kk6U5nSF8EggfmTpMdna96UuXWAVwSsDSXRkXeZ5vLcAFk77tr',
      seq: 4,
      signature: '3045022100b05e6738721100bf3eee37acab2ad60070bf9c7fa8e494f0d3f4de5eca291a220220677943ea5b1fcaadb71e6288a4a0982279c21bfbdcb3b0c3c26dffb84c27dd99',
      master_signature: '26e008edecbf7457f05fe569dbb6e0a6117da7d2e6903664647265429dae3acd5edc1fdbfdb792ff038086db46a98205d4d9ca73b8ef6a41ee42aa6ac3f5a70c'
    }).then(() => { done(); })
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

  it('should accept revocation without ephemeral key or signature', function(done) {
    manifests.handleManifest({
      master_key: 'nHB1PvPGSZhhNfdYDbwBmRmSWAEfd8YH97K9Bey82obyFh1nKDmq',
      seq: 4294967295,
      master_signature: 'A7D7E9C868874287B5888D5093DCAD8B8FF21BE306D9422C4DC7C143A37A2B961B490004892D6545A42A3A3291764B34A74D68FE1A0B22353E4F570F73C21401'
    }).then(() => { done(); })
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

  it('should require a master signature or signature', function(done) {
    manifests.handleManifest({
      master_key: 'nHU5wPBpv1kk3kafS2ML2GhyoGJuHhPP4fCa2dwYUjMT5wR8Dk5B',
      signing_key: 'n9LRZXPh1XZaJr5kVpdciN76WCCcb5ZRwjvHywd4Vc4fxyfGEDJA',
      seq: 4,
    }).catch((err) => {
      assert.strictEqual(err, 'master signature and signature cannot be null');
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

  it('should require a valid master signature', function(done) {
    manifests.handleManifest({
      master_key: 'nHUM1j7YGDVH7VbYw7bvjh9QR4f59GmQjwaNvnG34ki6U2upPYmY',
      signing_key: 'n9Kk6U5nSF8EggfmTpMdna96UuXWAVwSsDSXRkXeZ5vLcAFk77tr',
      seq: 4,
      signature: '3045022100b05e6738721100bf3eee37acab2ad60070bf9c7fa8e494f0d3f4de5eca291a220220677943ea5b1fcaadb71e6288a4a0982279c21bfbdcb3b0c3c26dffb84c27dd99',
      master_signature: 'badsig'
    }).catch((err) => {
      assert.strictEqual(err, 'Manifest has invalid signature');
      done();
    });
  });

  it('should not require a valid ephemeral signature', function(done) {
    manifests.handleManifest({
      master_key: 'nHUM1j7YGDVH7VbYw7bvjh9QR4f59GmQjwaNvnG34ki6U2upPYmY',
      signing_key: 'n9Kk6U5nSF8EggfmTpMdna96UuXWAVwSsDSXRkXeZ5vLcAFk77tr',
      seq: 4,
      signature: 'badsig',
      master_signature: '26e008edecbf7457f05fe569dbb6e0a6117da7d2e6903664647265429dae3acd5edc1fdbfdb792ff038086db46a98205d4d9ca73b8ef6a41ee42aa6ac3f5a70c'
    }).then(() => { done(); })
  });

  it('should store manifest with same seq and different ephemeral key on separate row', function(done) {
    const manifest1 = {
      master_signature: "DACAAA21F9516AFDDB80BD4AE36CDC31F39DFBA775E3F3C7936F8CA92A260C22742FB1D65A8A7FA9CB8B48051056A8CFEB23866FA74CA2EBF7593C2F5797C60D",
      master_key: "nHDaAY8AzhaySWMvCTRv62yup6jojk9MtmzuFeWKSSv1tGHLjk6C",
      seq: 4,
      signature: "3044022054C121DF2F3DF8818EB65DD376936BF6B8ED895D4954978C4153579979664F82022003BA3FD89FED67E4A20C67E2CEF1FD02DCBE42C86A6602DC42840BBF1DB20B05",
      signing_key: "n9JxsnSAkR8tuvNrKdMyKcyyZTVkiDePCDzofHqkbCW1bBua42BL"
    }

    const manifest2 = {
      master_signature: "99DBE499C0BA272C6B4AB940AC95EB4A17F2A6E6D7781124F2377AFDF1085E2EC78C42538C1D2178CF29F3A359BE17F5B8929CF44CCA0B03781E0DA5E4DA180B",
      master_key: "nHDaAY8AzhaySWMvCTRv62yup6jojk9MtmzuFeWKSSv1tGHLjk6C",
      seq: 4,
      signature: "3045022100FB93DFFB1D3F486B0E61593C0155744ADE24FEE7EA1C011C74837A842032A34402207F7F47A0F8DBB95C17A5EAEB4DBEA0A432A6A16A5424F97C4C4957FA174F075A",
      signing_key: "n9L3kP5evTeHUefVrWVBx6x8f815HwQ3zX7bZ1KrD8Crkx3upfNy"
    }

    manifests.handleManifest(manifest1)
    .then(() => {
      return manifests.handleManifest(manifest2)
    }).then(() => {
      return hbase.getAllRows({
        table: 'manifests_by_validator'
      });
    }).then((rows) => {
      assert.strictEqual(rows.length, 2);
      let manifests = {};
      for (const manifest of rows) {
        manifests[makeRowKey(manifest.master_public_key, manifest.sequence, manifest.ephemeral_public_key)] = manifest;
      }

      function testManifest(manifest) {
        const rowkey = makeRowKey(manifest.master_key, manifest.seq, manifest.signing_key);
        assert(manifests[rowkey]);
        assert.strictEqual(manifests[rowkey].rowkey, rowkey);
        assert.strictEqual(manifests[rowkey].master_public_key, manifest.master_key);
        assert.strictEqual(manifests[rowkey].sequence, manifest.seq.toString());
        assert.strictEqual(manifests[rowkey].ephemeral_public_key, manifest.signing_key);
        assert.strictEqual(manifests[rowkey].signature, manifest.signature);
        assert.strictEqual(manifests[rowkey].master_signature, manifest.master_signature);
        assert.strictEqual(manifests[rowkey].count, '1');
      }
      testManifest(manifest1);
      testManifest(manifest2);
      done()
    });
  });

  it('should update last_datetime for duplicate manifest', function(done) {
    const manifest = {
      signing_key: 'n9LRZXPh1XZaJr5kVpdciN76WCCcb5ZRwjvHywd4Vc4fxyfGEDJA',
      master_key: 'nHU5wPBpv1kk3kafS2ML2GhyoGJuHhPP4fCa2dwYUjMT5wR8Dk5B',
      seq: 4,
      signature: 'ba37041d4d9739ebf721a75f7a9e408d92b9920e71a6af5a9fe11e88f05c8937771e1811cf262f489b69c67cc80c96518a6e5c17091dd743246229d21ee2c00c'
    }

    let last_datetime

    manifests.handleManifest(manifest)
    .then(() => {
      return hbase.getAllRows({
        table: 'manifests_by_validator'
      });
    }).then((rows) => {
      assert.strictEqual(rows.length, 1);
      assert.strictEqual(rows[0].rowkey, makeRowKey(manifest.master_key, manifest.seq, manifest.signing_key));
      assert.strictEqual(rows[0].master_public_key, manifest.master_key);
      assert.strictEqual(rows[0].ephemeral_public_key, manifest.signing_key);
      assert.strictEqual(rows[0].sequence, manifest.seq.toString());
      assert.strictEqual(rows[0].signature, manifest.signature);
      assert.strictEqual(rows[0].count, '1');
      last_datetime = rows[0].last_datetime;
      return manifests.handleManifest(manifest)
    }).then(() => {
      //wait >1 second for updateManifest timer
      setTimeout(function() {
        hbase.getAllRows({
          table: 'manifests_by_validator'
        }).then((rows) => {
          assert.strictEqual(rows.length, 1);
          assert.strictEqual(rows[0].rowkey, makeRowKey(manifest.master_key, manifest.seq, manifest.signing_key));
          assert.strictEqual(rows[0].master_public_key, manifest.master_key);
          assert.strictEqual(rows[0].ephemeral_public_key, manifest.signing_key);
          assert.strictEqual(rows[0].sequence, manifest.seq.toString());
          assert.strictEqual(rows[0].signature, manifest.signature);
          assert.strictEqual(rows[0].count, '2');
          assert(last_datetime < rows[0].last_datetime)
          done();
        });
      }, 1010);
    })
  });

  it('should update row for duplicate manifest from different run', function(done) {
    const manifest = {
      signing_key: 'n9LRZXPh1XZaJr5kVpdciN76WCCcb5ZRwjvHywd4Vc4fxyfGEDJA',
      master_key: 'nHU5wPBpv1kk3kafS2ML2GhyoGJuHhPP4fCa2dwYUjMT5wR8Dk5B',
      seq: 4,
      signature: 'ba37041d4d9739ebf721a75f7a9e408d92b9920e71a6af5a9fe11e88f05c8937771e1811cf262f489b69c67cc80c96518a6e5c17091dd743246229d21ee2c00c'
    }

    let manifestsNew = new Manifests();

    manifests.handleManifest(manifest)
    .then(() => {
      return manifestsNew.start();
    }).then(() => {
      return manifestsNew.handleManifest(manifest)
    }).then(() => {
      //wait >1 second for updateManifest timer
      setTimeout(function() {
        hbase.getAllRows({
          table: 'manifests_by_validator'
        }).then((rows) => {
          assert.strictEqual(rows.length, 1);
          assert.strictEqual(rows[0].rowkey, makeRowKey(manifest.master_key, manifest.seq, manifest.signing_key));
          assert.strictEqual(rows[0].master_public_key, manifest.master_key);
          assert.strictEqual(rows[0].ephemeral_public_key, manifest.signing_key);
          assert.strictEqual(rows[0].sequence, manifest.seq.toString());
          assert.strictEqual(rows[0].signature, manifest.signature);
          assert.strictEqual(rows[0].count, '2');
          done();
        });
      }, 1010);
    })
  });

  it('should cache new manifests', function(done) {
    const master_public_key = 'nHU5wPBpv1kk3kafS2ML2GhyoGJuHhPP4fCa2dwYUjMT5wR8Dk5B'
    const old_ephemeral_public_key = 'n9KXuFUqkykLVr8oDwDeNuu33akSuUXShNER4y96Uco88R4xwpB5'
    const old_seq = 2
    const new_ephemeral_public_key = 'n9LRZXPh1XZaJr5kVpdciN76WCCcb5ZRwjvHywd4Vc4fxyfGEDJA'
    const new_seq = 4

    manifests.handleManifest({
      signing_key: old_ephemeral_public_key,
      master_key: master_public_key,
      seq: old_seq,
      signature: '58a01747386a7dc26e21512c52a7a01ef6ad2efc99fc2ecf0d288665f7bf7e831949abf7129dada2c47f5633ffa73a1a00d5fc061892ecead3a014a99924480e'
    }).then(() => {
      return manifests.handleManifest({
        signing_key: new_ephemeral_public_key,
        master_key: master_public_key,
        seq: new_seq,
        signature: 'ba37041d4d9739ebf721a75f7a9e408d92b9920e71a6af5a9fe11e88f05c8937771e1811cf262f489b69c67cc80c96518a6e5c17091dd743246229d21ee2c00c'
      });
    }).then(() => {
      return hbase.getAllRows({
        table: 'manifests_by_validator'
      });
    }).then((rows) => {
      assert.strictEqual(rows.length, 2);
      assert.strictEqual(rows[0].rowkey, makeRowKey(master_public_key, old_seq, old_ephemeral_public_key));
      assert.strictEqual(rows[0].master_public_key, master_public_key);
      assert.strictEqual(rows[0].ephemeral_public_key, old_ephemeral_public_key);
      assert.strictEqual(rows[0].sequence, old_seq.toString());
      assert.strictEqual(rows[1].rowkey, makeRowKey(master_public_key, new_seq, new_ephemeral_public_key));
      assert.strictEqual(rows[1].master_public_key, master_public_key);
      assert.strictEqual(rows[1].ephemeral_public_key, new_ephemeral_public_key);
      assert.strictEqual(rows[1].sequence, new_seq.toString());
      return hbase.getAllRows({
        table: 'manifests_by_master_key'
      });
    }).then((rows) => {
      assert.strictEqual(rows.length, 1);
      assert.strictEqual(rows[0].rowkey, master_public_key);
      assert.strictEqual(rows[0].ephemeral_public_key, new_ephemeral_public_key);
      assert.strictEqual(rows[0].sequence, new_seq.toString());
      done();
    });
  });

  it('should not cache new stale manifests', function(done) {
    const master_public_key = 'nHU5wPBpv1kk3kafS2ML2GhyoGJuHhPP4fCa2dwYUjMT5wR8Dk5B'
    const ephemeral_public_key = 'n9LRZXPh1XZaJr5kVpdciN76WCCcb5ZRwjvHywd4Vc4fxyfGEDJA'
    const seq = 4
    const stale_ephemeral_public_key = 'n9KXuFUqkykLVr8oDwDeNuu33akSuUXShNER4y96Uco88R4xwpB5'
    const stale_seq = 2

    manifests.handleManifest({
      signing_key: ephemeral_public_key,
      master_key: master_public_key,
      seq: seq,
      signature: 'ba37041d4d9739ebf721a75f7a9e408d92b9920e71a6af5a9fe11e88f05c8937771e1811cf262f489b69c67cc80c96518a6e5c17091dd743246229d21ee2c00c'
    }).then(() => {
      return manifests.handleManifest({
        signing_key: stale_ephemeral_public_key,
        master_key: master_public_key,
        seq: stale_seq,
        signature: '58a01747386a7dc26e21512c52a7a01ef6ad2efc99fc2ecf0d288665f7bf7e831949abf7129dada2c47f5633ffa73a1a00d5fc061892ecead3a014a99924480e'
      });
    }).then(() => {
      return hbase.getAllRows({
        table: 'manifests_by_validator'
      });
    }).then((rows) => {
      assert.strictEqual(rows.length, 2);
      assert.strictEqual(rows[0].rowkey, makeRowKey(master_public_key, stale_seq, stale_ephemeral_public_key));
      assert.strictEqual(rows[0].master_public_key, master_public_key);
      assert.strictEqual(rows[0].ephemeral_public_key, stale_ephemeral_public_key);
      assert.strictEqual(rows[0].sequence, stale_seq.toString());
      assert.strictEqual(rows[1].rowkey, makeRowKey(master_public_key, seq, ephemeral_public_key));
      assert.strictEqual(rows[1].master_public_key, master_public_key);
      assert.strictEqual(rows[1].ephemeral_public_key, ephemeral_public_key);
      assert.strictEqual(rows[1].sequence, seq.toString());
      return hbase.getAllRows({
        table: 'manifests_by_master_key'
      });
    }).then((rows) => {
      assert.strictEqual(rows.length, 1);
      assert.strictEqual(rows[0].rowkey, master_public_key);
      assert.strictEqual(rows[0].ephemeral_public_key, ephemeral_public_key);
      assert.strictEqual(rows[0].sequence, seq.toString());
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
      assert.strictEqual(rows[0].rowkey, manifest.master_public_key);
      assert.strictEqual(rows[0].ephemeral_public_key, manifest.ephemeral_public_key);
      assert.strictEqual(rows[0].sequence, manifest.sequence.toString());
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

describe('validator manifests endpoint', function() {

  before(function(done) {
    manifests = new Manifests();
    hbase.deleteAllRows({
      table: 'manifests_by_master_key'
    }).then(() => {
      return hbase.deleteAllRows({
        table: 'manifests_by_validator'
      })
    }).then(() => {
      Promise.map(mockManifests, function(man) {
        return manifests.handleManifest(man)
      })
    }).then(() => { done(); })
  });

  it('should get validator manifest', function(done) {

    var pubkey = 'nHBV75zgMXCRHiuTMq6MdbcA6tBoSMWucTvHrnkQFW9gAXWoW15N'
    var url = 'http://localhost:' + port +
        '/v2/network/validators/' + pubkey + '/manifests'

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(body.manifests.length, 1)
      _.isMatch(body.manifests[0], mockResponses[pubkey][0])
      assert.strictEqual(body.manifests[0].count, 1)
      done()
    });
  });

  it('should get multiple validator manifests', function(done) {
    var pubkey = 'nHDEmQKb2nbcewdQ1fqCTGcPTcePhJ2Rh6MRftsCaf6UNRQLv7pB'
    var url = 'http://localhost:' + port +
        '/v2/network/validators/' + pubkey + '/manifests'

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(body.manifests.length, 2)
      _.isMatch(body.manifests[0], mockResponses[pubkey][0])
      _.isMatch(body.manifests[1], mockResponses[pubkey][1])
      done()
    });
  });

  it('should use limit and marker', function(done) {
    var pubkey = 'nHDEmQKb2nbcewdQ1fqCTGcPTcePhJ2Rh6MRftsCaf6UNRQLv7pB'
    var url = 'http://localhost:' + port +
        '/v2/network/validators/' + pubkey + '/manifests'

    request({
      url: url + '?limit=1',
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(body.manifests.length, 1)
      _.isMatch(body.manifests[0], mockResponses[pubkey][0])
      request({
        url: url + '?marker=' + body.marker,
        json: true
      },
      function(err, res, body) {
        assert.ifError(err)
        assert.strictEqual(res.statusCode, 200)
        assert.strictEqual(body.manifests.length, 1)
        _.isMatch(body.manifests[0], mockResponses[pubkey][1])
        done()
      })
    });
  });

  it('should get validator revocation manifests', function(done) {
    var pubkey = 'nHUtR1DUzB5AbHFDTwByTF684SwvyDxDqwcsBavZR62VFESMCBHj'
    var url = 'http://localhost:' + port +
        '/v2/network/validators/' + pubkey + '/manifests?descending=true'

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(body.manifests.length, 2)
      _.isMatch(body.manifests[0], mockResponses[pubkey][1])
      _.isMatch(body.manifests[1], mockResponses[pubkey][0])
      done()
    });
  });


  it('should get manifests in CSV format', function(done) {
    var pubkey = 'nHDEmQKb2nbcewdQ1fqCTGcPTcePhJ2Rh6MRftsCaf6UNRQLv7pB'
    var url = 'http://localhost:' + port +
        '/v2/network/validators/' + pubkey + '/manifests?format=csv'

    request({
      url: url
    },
    function(err, res) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(res.headers['content-disposition'],
        'attachment; filename=manifests.csv')
      done()
    })
  })

  it('should get empty list if no manifest found', function(done) {
    var pubkey = 'zzz'
    var url = 'http://localhost:' + port +
        '/v2/network/validators/' + pubkey + '/manifests'

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(body.manifests.length, 0)
      done()
    })
  })
});
