var hexMatch    = new RegExp('(0x)?[0-9a-f]+');
var base64Match = new RegExp('^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{4})([=]{1,2})?$');
var sjcl        = require('ripple-lib').sjcl;

var Memos = function (tx) {
  var list = [];

//NOTE: keep all memos  
//  if ( tx.metaData.TransactionResult !== "tesSUCCESS" ) {
//    return list;
//  }

  
  if (tx.Memos) {
    tx.Memos.forEach(function(memo, i) {
      var data = {
        account : tx.Account,
        type    : memo.Memo.MemoType,
        data    : memo.Memo.MemoData,
      };
      
      if (memo.Memo.MemoFormat) {
        data.format = memo.Memo.MemoFormat;
      }
      
      if (tx.Destination) {
        data.destination = tx.Destination;
      }
      
      if (tx.DestinationTag) {
        data.destination_tag = tx.DestinationTag;
      }
      
      if (tx.SourceTag) {
        data.source_tag = tx.SourceTag;
      }
      
      
      //attempt to decode from base64 or hex
      try {
        if (hexMatch.test(data.data)) {
          data.data = decodeHex(data.data);
          data.encoding = 'hex';

        } else if (base64Match.test(data.data)) {
          data.data = decodeBase64(data.data);  
          data.encoding = 'base64';
        }

        if (hexMatch.test(data.type)) {
          data.type = decodeHex(data.type);
          data.type_encoding = 'hex';

        } else if (base64Match.test(data.type)) {
          data.type = decodeBase64(data.type);  
          data.type_encoding = 'base64';
        }  
      } catch (e) {
        //unable to decode
      }
      
      data.time         = tx.executed_time;
      data.ledger_index = tx.ledger_index; 
      data.tx_index     = tx.tx_index;
      data.memo_index   = i,
      data.tx_hash      = tx.hash;
      
      list.push(data);
    });
  }
  
  return list;
};

function decodeBase64(data) {
  return sjcl.codec.utf8String.fromBits(sjcl.codec.base64.toBits(data));
}

function decodeHex(data) {
  return sjcl.codec.utf8String.fromBits(sjcl.codec.hex.toBits(data));
}

module.exports = Memos;