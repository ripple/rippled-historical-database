var log  = require('../log')('Ledger_Parser');
var SerializedObject = require('ripple-lib').SerializedObject;
var binformat = require('../../node_modules/ripple-lib/src/js/ripple/binformat');
var utils = require('./utils');

var EPOCH_OFFSET = 946684800;
var LI_PAD       = 12;
var I_PAD        = 5;
var E_PAD        = 3;
var S_PAD        = 12;
var Parser       = { };

var TX_TYPES   = { };
var TX_RESULTS = { };

Object.keys(binformat.tx).forEach(function(key) {
  TX_TYPES[key] = binformat.tx[key][0];
});

Object.keys(binformat.ter).forEach(function(key) {
  TX_RESULTS[key] = binformat.ter[key];
});

Parser.affectedAccounts = require('./affectedAccounts');
Parser.offersExercised  = require('./offersExercised');
Parser.balanceChanges   = require('./balanceChanges');
Parser.accountsCreated  = require('./accountsCreated');
Parser.memos            = require('./memos');
Parser.payment          = require('./payment');

Parser.parseHBase = function (ledger) {
  var tables = { 
    ledgers          : { },
    ledgers_by_index : { },
    ledgers_by_time  : { },    
    transactions     : { },
    transactions_by_time : { },
    transactions_by_account_sequence : { },
    transactions_by_affected_account : { },
    offers_exercised : { },
    offers_exercised_by_account : { },
    balance_changes  : { },
    balance_changes_by_account : { },
    value_moved      : { },
    value_moved_by_account : { },
    payments         : { },
    payments_by_account : { },
    accounts_created : { },
    accounts_created_by_parent : { },
    memos : { }
  };
  
  var data = this.parse(ledger);
  var ledgerIndexKey = pad(data.ledger.ledger_index, LI_PAD) + '|' + data.ledger.ledger_hash;
  var ledgerTimeKey  = utils.formatTime(ledger.close_time) + '|' + pad(data.ledger.ledger_index, LI_PAD);
  
  //add formated ledger
  tables.ledgers[ledger.ledger_hash] = data.ledger;
  
  //add ledger index lookup
  tables.ledgers_by_index[ledgerIndexKey] = {
    ledger_hash : data.ledger.ledger_hash,
    close_time  : data.ledger.close_time
  }
  
  //add ledger by time lookup
  tables.ledgers_by_time[ledgerTimeKey] = {
    ledger_hash : data.ledger.ledger_hash,
    ledger_index : data.ledger.ledger_index
  }  
  
  //add transactions
  data.transactions.forEach(function(tx) {
    var type   = pad(TX_TYPES[tx.TransactionType], E_PAD);
    var result = pad(TX_RESULTS[tx.tx_result], E_PAD); 
    var key;

    //add transaction
    tables.transactions[tx.hash] = tx;
    
    //transactions by time
    key = type + 
      '|' + result + 
      '|' + utils.formatTime(tx.executed_time) + 
      '|' + pad(data.ledger.ledger_index, LI_PAD) + 
      '|' + pad(tx.tx_index, I_PAD);
    
    tables.transactions_by_time[key] = {
      tx_hash : tx.hash
    }

    //transactions by account sequence
    key = tx.Account +
      '|' + type +
      '|' + result + 
      '|' + pad(tx.Sequence, S_PAD);
    
    tables.transactions_by_account_sequence[key] = {
      tx_hash       : tx.hash,
      executed_time : tx.executed_time,
      ledger_index  : data.ledger.ledger_index
    }
  });
  
  data.offersExercised.forEach(function(ex) {
    var key = ex.base.currency + 
      '|' + (ex.base.issuer || '') + 
      '|' + ex.counter.currency + 
      '|' + (ex.counter.issuer || '') + 
      '|' + utils.formatTime(ex.time) + 
      '|' + pad(ex.ledger_index, LI_PAD) + 
      '|' + pad(ex.tx_index, I_PAD) +
      '|' + pad(ex.node_index, I_PAD); //guarantee uniqueness
    
    var key2 = ex.account + '|' + key;
    var key3 = ex.counterparty + '|' + key;
    
    tables.offers_exercised[key] = {
        base_amount    : ex.base.amount,
        counter_amount : ex.counter.amount,
        rate           : ex.rate,
        account        : ex.account,
        counterparty   : ex.counterparty,
        tx_hash        : ex.tx_hash
    }
    
    tables.offers_exercised_by_account[key2] = {
      base_amount    : ex.base.amount,
      counter_amount : ex.counter.amount,
      rate           : ex.rate,
      counterparty   : ex.counterparty,
      taker          : true,      
      tx_hash        : ex.tx_hash,
    };
    
    
    tables.offers_exercised_by_account[key3] = {
      base_amount    : ex.base.amount,
      counter_amount : ex.counter.amount,
      rate           : ex.rate,
      counterparty   : ex.account,
      taker          : false,
      tx_hash        : ex.tx_hash
    };
    
  });
  
  data.balanceChanges.forEach(function(c) {
    var key = c.currency +
      '|' + (c.issuer ||  '') +
      '|' + utils.formatTime(c.time) + 
      '|' + pad(c.ledger_index, LI_PAD) + 
      '|' + pad(c.tx_index, I_PAD) +
      '|' + (c.node_index === 'fee' ? 'fee' : pad(c.node_index, I_PAD));
    
    tables.balance_changes[key] = {
      account : c.account,
      change : c.change,
      final_balance : c.final_balance,
      type : c.type,
      tx_hash : c.tx_hash
    };
    
    key = c.account + '|' + key;
    tables.balance_changes_by_account[key] = {
      change : c.change,
      final_balance : c.final_balance,
      type : c.type,
      tx_hash : c.tx_hash
    };    
      
  });
  
  data.payments.forEach(function(p) {
    var key = p.currency +
      '|' + utils.formatTime(p.time) + 
      '|' + pad(p.ledger_index, LI_PAD) + 
      '|' + pad(p.tx_index, I_PAD);
    
    var payment = {
      source      : p.source,
      destination : p.destination,
      amount      : p.amount,
      fee         : p.fee,
      source_balance_changes : p.source_balance_changes,
      destination_balance_changes : p.destination_balance_changes,
      tx_hash  : p.tx_hash
    }
    
    if (p.destination_tag) {
      payment.destination_tag = p.destination_tag;
    }
    
    if (p.source_tag) {
      payment.source_tag = p.source_tag;
    }
    
    tables.payments[key] = payment;
    tables.payments_by_account[p.source      + '|' + key] = payment;
    tables.payments_by_account[p.destination + '|' + key] = payment;
  });
  
  data.accountsCreated.forEach(function(a) {
    var key = utils.formatTime(a.time) + 
      '|' + pad(a.ledger_index, LI_PAD) + 
      '|' + pad(a.tx_index, I_PAD);
    
    tables.accounts_created[key] = {
      account : a.account,
      parent  : a.parent,
      balance : a.balance,
      tx_hash : a.tx_hash
    };
    
    tables.accounts_created_by_parent[a.parent + '|' + key] = {
      account : a.account,
      balance : a.balance,
      tx_hash : a.tx_hash
    };    
  });
  
  data.memos.forEach(function(m) {
    var key = utils.formatTime(m.time) + 
      '|' + pad(m.ledger_index, LI_PAD) + 
      '|' + pad(m.tx_index, I_PAD) + 
      '|' + pad(m.memo_index, I_PAD);
    
    delete m.time;
    delete m.ledger_index;
    delete m.tx_index;
    delete m.memo_index;
    
    tables.memos[m.account + '|' + key] = m;
    if (m.destination) {
      tables.memos[m.destination + '|' + key] = m;
    }
  });
  
  data.affectedAccounts.forEach(function(a) {
    var key = a.account + 
      '|' + pad(TX_TYPES[a.tx_type], E_PAD) +
      '|' + pad(TX_RESULTS[a.tx_result], E_PAD) +
      '|' + utils.formatTime(a.time) + 
      '|' + pad(a.ledger_index, LI_PAD) + 
      '|' + pad(a.tx_index, I_PAD);  
    
    tables.transactions_by_affected_account[key] = {
      tx_hash : a.tx_hash,
    }
  });
  
  //console.log(tables.offers_exercised);
  //console.log(tables.offers_exercised_by_account);
  //console.log(tables.payments);
  //console.log(tables.payments_by_account);
  //console.log(tables.ledgers_by_index);
  //console.log(tables.ledgers_by_time);
  //console.log(tables.balance_changes);
  //console.log(tables.accounts_created);
  //console.log(tables.accounts_created_by_parent);
  //console.log(tables.memos);
  //console.log(tables);
  
  return tables;
};

Parser.parse = function(ledger) {
  var data = {
    ledger           : null,
    transactions     : [],
    affectedAccounts : [],
    accountsCreated  : [],
    offersExercised  : [],
    balanceChanges   : [],
    payments         : [],
    memos            : [],
    valueMoved       : []
  }
  
  var transactions = ledger.transactions; 
  
  ledger.close_time   = ledger.close_time + EPOCH_OFFSET;
  ledger.transactions = [];
  
  transactions.forEach(function(transaction) {
    ledger.transactions.push(transaction.hash);
    var meta = transaction.metaData;
    var payment;
    
    delete transaction.metaData;
    
    try {
      transaction.raw  = toHex(transaction);
      transaction.meta = toHex(meta);
      
    } catch (e) {
      log.error(e, transaction.ledger_index, transaction.hash);
      return;
    }
    
    transaction.metaData        = meta;
    transaction.ledger_hash     = ledger.ledger_hash;
    transaction.ledger_index    = ledger.ledger_index;
    transaction.executed_time   = ledger.close_time;
    transaction.tx_index        = transaction.metaData.TransactionIndex;
    transaction.tx_result       = transaction.metaData.TransactionResult;
    
    data.transactions.push(transaction);
    
    data.offersExercised  = data.offersExercised.concat(Parser.offersExercised(transaction));
    data.balanceChanges   = data.balanceChanges.concat(Parser.balanceChanges(transaction));
    data.accountsCreated  = data.accountsCreated.concat(Parser.accountsCreated(transaction));
    data.affectedAccounts = data.affectedAccounts.concat(Parser.affectedAccounts(transaction)); 
    data.memos            = data.memos.concat(Parser.memos(transaction)); 
    payment = Parser.payment(transaction);
    if (payment) {
      data.payments.push(payment);
    }
    
    //memos
    //addAccountsCreated(transaction);
    //addOffersExercised(transaction);
    //addBalanceChanges(transaction);
    //addAccountTransactions(transaction);
    //addTransaction(transaction);
  });
  
  data.ledger = ledger;
  
/* 
  console.log("ACCOUNTS CREATED");
  console.log(data.accountsCreated); 
  console.log("BALANCE CHANGES");
  console.log(data.balanceChanges);  
  console.log("OFFERS EXERCISED");
  console.log(data.offersExercised);
  console.log("AFFECTED ACCOUNTS");
  console.log(data.affectedAccounts);
  //console.log(data.transactions);
  console.log('done');
*/

  return data;
  
  /**
   * addTransaction
   * format a transaction and add it to
   * the parsed data
   */
  
  function addTransaction (transaction) {
   var key = transaction.hash + 
    '|t|'  + utils.formatTime(transaction.executed_time) + 
    '|ti|' + pad(transaction.tx_index, I_PAD);
    
    data.transactions[key] = transaction;
    
    //add to lookup table
    key = pad(transaction.ledger_index, LI_PAD) + '|ti|' + pad(transaction.tx_index, I_PAD);
    data.transactionsByIndex[key] = {tx_hash: transaction.hash};
  }
  
  /**
   * addOffersExercised
   * add all offers exercised found
   */
  
  function addOffersExercised (transaction) {
    var list = Parser.offersExercised(transaction);
    
    list.forEach(function(ex, i) {
      var c1  = ex.base.currency + (ex.base.issuer ? "." + ex.base.issuer : "");
      var c2  = ex.counter.currency + (ex.counter.issuer ? "." + ex.counter.issuer : "");
      var key = c1 + '/' + c2 + 
        '|t|'  + utils.formatTime(transaction.executed_time) + 
        '|ti|' + pad(transaction.tx_index, I_PAD) +
        '|n|'  + pad(ex.nodeIndex, I_PAD); //guarantee uniqueness

      
      var value = {
        baseAmount    : ex.base.amount,
        counterAmount : ex.counter.amount,
        rate          : ex.rate,
        account       : ex.account,
        counterparty  : ex.counterparty,
        tx_hash       : transaction.hash
      };
            
      data.offersExercised[key] = value;
      
      addAffectedAccount('exchange', ex.account, transaction);
      addAffectedAccount('exchange', ex.counterparty, transaction);
    });
  }
  
  /**
   * addAffectedAccount
   * add an account affected by the transaction
   * to the account transactions list
   */
  
  function addAffectedAccount (role, account, transaction) {
    var key = account +
        '|r|'  + transaction.tx_result + 
        '|tp|' + transaction.TransactionType +
        '|t|'  + utils.formatTime(transaction.executed_time) + 
        '|ti|' + pad(transaction.tx_index, I_PAD);
    
    if (!data.accountTx[key]) {
      data.accountTx[key] = {
        roles     : role ? [role] : [],
        tx_hash   : transaction.hash,
      };
    
      //add to lookup table
      key = account + '|li|' + pad(transaction.ledger_index, LI_PAD) + '|ti|' + pad(transaction.tx_index, I_PAD);
      data.accountTxByIndex[key] = {tx_hash: transaction.hash};
      
    } else if (role && data.accountTx[key].roles.indexOf(role) === -1) {
      data.accountTx[key].roles.push(role);
    }
    
    if (!data.accounts[account]) {
      data.accounts[account] = { };
    }
  }
  
  /**
   * addAccountTransactions
   * add affected transactions to
   * account transactions lookup table
   */
  
  function addAccountTransactions (transaction) {
    var accounts = Parser.affectedAccounts(transaction);
    accounts.forEach(function(account) {
      if (account.roles.length) {
        account.roles.forEach(function(role) {
          addAffectedAccount(role, account.account, transaction); 
        });
        
      } else {
        addAffectedAccount(null, account.account, transaction);  
      }
    });
  }
  
  /**
   * addAccountBalanceChanges
   * add all balance changes found
   */
  
  function addBalanceChanges (transaction) {
    var list = Parser.balanceChanges(transaction);
    list.forEach(function(change, i) {
      var key = change.account + 
        '|c|'  + change.currency + 
        '|i|'  + (change.issuer || "")  +   
        '|t|'  + utils.formatTime(transaction.executed_time) + 
        '|ti|' + pad(transaction.tx_index, I_PAD) +
        '|ni|' + pad(change.nodeIndex, I_PAD); //guarantee uniqueness  
      
      data.balanceChanges[key] = {
        change  : change.change,
        amount  : change.amount,
        fee     : change.fee,
        balance : change.balance,
        type    : change.type,
      }
      
      addAffectedAccount('balanceChange', change.account, transaction); 
      if (change.issuer) {
        addAffectedAccount('balanceChange', change.issuer, transaction); 
      }
    });
    
    if (transaction.TransactionType === 'Payment') {
      var payment = { };
      list.forEach(function(change) {

        if (change.type === 'network fee') {
          payment.networkFee = 0 - change.fee;
        }
        
        if (change.type !== 'payment') {
          return;
        }
        
        if (change.account === transaction.Account) {
          payment.sender = change.account;
          payment.amountSent = {
            amount   : 0 - change.amount,
            currency : change.currency
          };
          
          if (change.issuer) {
            payment.amountSent.issuer = change.issuer;
          }
          
          payment.senderBalance = change.balance;
        
        } else if (change.account === transaction.Destination) {
          payment.receiver = change.account;
          payment.amountReceived = {
            amount   : change.amount,
            currency : change.currency
          };
          
          if (change.issuer) {
            payment.amountReceived.issuer = change.issuer;
          }
          
          payment.receiverBalance = change.balance;          
        }
      });
      
      //issuer is the sender
      if (payment.sender && !payment.receiver) {
        payment.amountReceived = payment.amountSent;
        payment.receiver = payment.amountReceived.issuer;
        payment.receiverBalance = 0 - payment.senderBalance;
      
      //issuer is the receiver
      } else if (payment.receiver && !payment.sender) {
        payment.amountSent = payment.amountReceived;
        payment.sender = payment.amountSent.issuer;
        payment.senderBalance = 0 - payment.receiverBalances;      
      }
      
      payment.tx_hash = transaction.hash;
    }
  }
  
  /**
   * addAccountsCreated
   * add any new accounts created
   * on this ledger
   */
  
  function addAccountsCreated (transaction) {
    var list = Parser.accountsCreated(transaction);
    list.forEach(function(account) {
      var key = account.account + 
        '|p|'  + account.parent +
        '|t|'  + utils.formatTime(transaction.executed_time) + 
        '|ti|' + pad(transaction.tx_index, I_PAD);
      
      data.accountsCreated[key] = {tx_hash : transaction.hash};
      
      key = account.account;
      data.accounts[key] = {
        parent    : account.parent,
        created   : transaction.executed_time,
        create_tx : transaction.hash,
      };
      
      addAffectedAccount('created', account.account, transaction); 
      addAffectedAccount('parent', account.parent, transaction);
    });
  }
  
  //Convert json to binary/hex to store as raw data
  function toHex(input){
    return new SerializedObject.from_json(input).to_hex();
  }
};


function pad(num, size) {
  var s = num+"";
  if (!size) size = 10;
  while (s.length < size) s = "0" + s;
  return s;
}

module.exports = Parser;