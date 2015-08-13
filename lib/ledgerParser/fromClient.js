var hexMatch    = new RegExp('^(0x)?[0-9A-Fa-f]+$');
var base64Match = new RegExp('^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{4})([=]{1,2})?$');
var sjcl        = require('sjcl');

var FromClient = function (tx) {
  var client;

  if (!tx.Memos) {
    return;
  }

  client = getClientString();

  //return max 100 chars
  return client ? client.substring(0,100) : undefined;

  function getClientString() {
    var memo;
    var type;

    for (var i=0; i<tx.Memos.length; i++) {
      memo = tx.Memos[i].Memo;


      try {
        // look for 'client' in MemoType
        if (hexMatch.test(memo.MemoType)) {
          type = decodeHex(memo.MemoType).toLowerCase();

        } else if (base64Match.test(memo.MemoType)) {
          type = decodeBase64(memo.MemoType).toLowerCase();

        } else {
          continue;
        }

        if (type !== 'client') {
          continue;
        }

        //check MemoData
        if (memo.MemoData) {
          if (hexMatch.test(memo.MemoData)) {
            return decodeHex(memo.MemoData);

          } else if (base64Match.test(memo.MemoData)) {
            return decodeBase64(memo.MemoData);
          }

        //check MemoFormat
        } else if (memo.MemoFormat) {
          if (hexMatch.test(memo.MemoFormat)) {
            return decodeHex(memo.MemoFormat);

          } else if (base64Match.test(memo.MemoFormat)) {
            return decodeBase64(memo.MemoFormat);
          }
        }

      } catch (e) {
        //unable to decode
      }
    }

    return;
  }
};

function decodeBase64(data) {
  return sjcl.codec.utf8String.fromBits(sjcl.codec.base64.toBits(data));
}

function decodeHex(data) {
  return sjcl.codec.utf8String.fromBits(sjcl.codec.hex.toBits(data));
}

module.exports = FromClient;
