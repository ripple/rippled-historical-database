var hexMatch    = new RegExp('^(0x)?[0-9A-Fa-f]+$');
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
      
      if (!memo.Memo) return;
      
      var data = {
        account : tx.Account,
        memo_type    : memo.Memo.MemoType,
        memo_data    : memo.Memo.MemoData,
      };
      
      if (memo.Memo.MemoFormat) {
        data.memo_format = memo.Memo.MemoFormat;
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
        if (hexMatch.test(data.memo_data)) {
          data.decoded_data = decodeHex(data.memo_data);
          data.data_encoding = 'hex';

        } else if (base64Match.test(data.memo_data)) {
          data.decoded_data = decodeBase64(data.memo_data);  
          data.data_encoding = 'base64';
        }

        if (hexMatch.test(data.memo_type)) {
          data.decoded_type = decodeHex(data.memo_type);
          data.type_encoding = 'hex';

        } else if (base64Match.test(data.memo_type)) {
          data.decoded_type = decodeBase64(data.memo_type);  
          data.type_encoding = 'base64';
        } 
        
        if (data.memo_format && hexMatch.test(data.memo_format)) {
          data.decoded_format = decodeHex(data.memo_fromat);
          data.format_encoding = 'hex';

        } else if (data.memo_format && base64Match.test(data.memo_format)) {
          data.decoded_format = decodeBase64(data.memo_format);  
          data.formata_encoding = 'base64';
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